use std::fs::File;

use arrow2::array::{Array, Int32Array, Utf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::error::Result;
use arrow2::io::fuse::write;
use std::io::Write;

fn write_batches(path: &str, schema: Schema, chunks: &[Chunk<Box<dyn Array>>]) -> Result<()> {
    let file = File::create(path)?;

    let options = write::WriteOptions {
        compression: Some(write::Compression::LZ4),
        max_page_size: Some(8192),
    };
    let mut writer = write::FuseWriter::new(file, schema, options);

    writer.start()?;
    for chunk in chunks {
        writer.write(chunk)?
    }

    writer.finish();

    let metas = serde_json::to_vec(&writer.metas).unwrap();
    let mut meta_file = File::options()
        .create(true)
        .write(true)
        .truncate(true)
        .open("/tmp/fuse.meta")?;
    meta_file.write_all(&metas)?;
    meta_file.flush();
    Ok(())
}

// cargo run --package arrow2 --example fuse_file_write --features io_json_integration,io_fuse,io_parquet,io_parquet_compression,io_ipc_compression /tmp/input.fuse
fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];
    let (chunk, schema) = read_chunk();
    // write it
    write_batches(file_path, schema, &[chunk])?;

    Ok(())
}

fn read_chunk() -> (Chunk<Box<dyn Array>>, Schema) {
    let file_path = "/tmp/input.parquet";
    let mut reader = File::open(file_path).unwrap();

    // we can read its metadata:
    let metadata = arrow2::io::parquet::read::read_metadata(&mut reader).unwrap();
    // and infer a [`Schema`] from the `metadata`.
    let schema = arrow2::io::parquet::read::infer_schema(&metadata).unwrap();
    // we can filter the columns we need (here we select all)
    let schema = schema.filter(|_index, _field| true);

    // we can read the statistics of all parquet's row groups (here for each field)
    for field in &schema.fields {
        let statistics =
            arrow2::io::parquet::read::statistics::deserialize(field, &metadata.row_groups)
                .unwrap();
        println!("{:#?}", statistics);
    }

    // say we found that we only need to read the first two row groups, "0" and "1"
    let row_groups = metadata
        .row_groups
        .into_iter()
        .enumerate()
        .filter(|(index, _)| *index == 0 || *index == 1)
        .map(|(_, row_group)| row_group)
        .collect();

    // we can then read the row groups into chunks
    let chunks = arrow2::io::parquet::read::FileReader::new(
        reader,
        row_groups,
        schema.clone(),
        Some(usize::MAX),
        None,
        None,
    );

    for maybe_chunk in chunks {
        let chunk = maybe_chunk.unwrap();
        println!("chunk len -> {:?}", chunk.len());
        return (chunk, schema);
    }
    unreachable!()
}
