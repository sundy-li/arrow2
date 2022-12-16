use std::io::{BufReader, Read};

use crate::array::{Offset, Utf8Array};
use crate::buffer::Buffer;
use crate::datatypes::DataType;
use crate::error::Result;
use crate::io::fuse::read::{Compression, FuseReadBuf};

use super::super::read_basic::*;

pub fn read_utf8<O: Offset, R: FuseReadBuf>(
    reader: &mut R,
    data_type: DataType,
    is_little_endian: bool,
    compression: Option<Compression>,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<Utf8Array<O>> {
    let validity = read_validity(reader, is_little_endian, compression, length, scratch)?;

    let offsets: Buffer<O> =
        read_buffer(reader, is_little_endian, compression, 1 + length, scratch)?;

    let last_offset = offsets.last().unwrap().to_usize();
    let values = read_buffer(reader, is_little_endian, compression, last_offset, scratch)?;

    Utf8Array::<O>::try_new(data_type, offsets, values, validity)
}
