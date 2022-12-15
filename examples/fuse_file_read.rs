use std::fs::File;
use std::io::{Seek, Read};
use std::time::Instant;

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::error::Result;
use arrow2::io::fuse::read::deserialize;
use arrow2::io::fuse::{read, ColumnMeta};

/// Simplest way: read all record batches from the file. This can be used e.g. for random access.
#[allow(clippy::type_complexity)]
fn read_chunks(path: &str) -> Result<(Schema,  Chunk<Box<dyn Array>>)> {
    let mut reader = File::open( "/tmp/input.parquet").unwrap();
    // we can read its metadata:
    let metadata = arrow2::io::parquet::read::read_metadata(&mut reader).unwrap();
    // and infer a [`Schema`] from the `metadata`.
    let schema = arrow2::io::parquet::read::infer_schema(&metadata).unwrap();
    
    let meta = File::open("/tmp/fuse.meta").unwrap();
    let metas: Vec<ColumnMeta> = serde_json::from_reader(meta).unwrap();
    
    let mut chunks =  vec![];
    
    for (meta, field) in metas.iter().zip(schema.fields.iter()) {
        let mut reader = File::open(path).unwrap();
        reader.seek(std::io::SeekFrom::Start(meta.offset)).unwrap();
        let mut reader = reader.take(meta.length);
        
        // println!("{:?}", field.data_type());
       let array = deserialize::read(&mut reader, field.data_type().clone(), true, Some(read::Compression::LZ4), meta.num_values as usize)?;
       chunks.push(array);
    }
    Ok((schema, Chunk::new(chunks)))
}

// cargo run --package arrow2 --example fuse_file_read --features io_json_integration,io_fuse,io_parquet,io_parquet_compression,io_ipc_compression  --release /tmp/input.fuse 
fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let t = Instant::now();
    let (schema, chunks) = read_chunks(file_path)?;
    let names = schema.fields.iter().map(|f| &f.name).collect::<Vec<_>>();

    println!(" rows: {}", chunks.len());
    println!("cost {:?} ms", t.elapsed().as_millis());
    // println!("{}", print::write(&[chunks], &["names", "tt", "3", "44"]));
    Ok(())
}
