use std::fs::File;
use std::io::{Read, Seek, BufReader};
use std::time::Instant;

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::error::Result;
use arrow2::io::fuse::read::deserialize;
use arrow2::io::fuse::read::reader::FuseReader;
use arrow2::io::fuse::{read, ColumnMeta};

/// Simplest way: read all record batches from the file. This can be used e.g. for random access.
// cargo run --package arrow2 --example fuse_file_read --features io_json_integration,io_fuse,io_parquet,io_parquet_compression,io_ipc_compression  --release /tmp/input.fuse
fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let t = Instant::now();
    {
        let mut reader = File::open("/tmp/input.parquet").unwrap();
        // we can read its metadata:
        let metadata = arrow2::io::parquet::read::read_metadata(&mut reader).unwrap();
        // and infer a [`Schema`] from the `metadata`.
        let schema = arrow2::io::parquet::read::infer_schema(&metadata).unwrap();

        let meta = File::open("/tmp/fuse.meta").unwrap();
        let metas: Vec<ColumnMeta> = serde_json::from_reader(meta).unwrap();

        let mut readers = vec![];
        for (meta, field) in metas.iter().zip(schema.fields.iter()) {
            let mut reader = File::open("/tmp/input.fuse").unwrap();
            reader.seek(std::io::SeekFrom::Start(meta.offset)).unwrap();
            let reader = reader.take(meta.length);
            
            let buffer_size = meta.length.min(8192) as usize;
            let reader = BufReader::with_capacity( buffer_size, reader);
            let mut scratch = Vec::with_capacity(8 * 1024);
            
            let fuse_reader = FuseReader::new(
                reader,
                field.data_type().clone(),
                true,
                Some(read::Compression::LZ4),
                meta.num_values as usize,
                scratch
            );
            
            readers.push(fuse_reader);
        }
        
        'FOR: loop {
            let mut chunks = Vec::new();
            for reader in readers.iter_mut() {
                if !reader.has_next() {
                    break 'FOR
                }
                chunks.push(reader.next_array().unwrap());
            }
            
            let chunk = Chunk::new(chunks);
            println!("READ -> {:?} rows", chunk.len());
        }
    }

    println!("cost {:?} ms", t.elapsed().as_millis());
    // println!("{}", print::write(&[chunks], &["names", "tt", "3", "44"]));
    Ok(())
}
