use std::io::{BufReader, Read};

use crate::array::{BinaryArray, Offset};
use crate::buffer::Buffer;
use crate::datatypes::DataType;
use crate::error::Result;
use crate::io::fuse::read::{Compression, FuseReadBuf};

use super::super::read_basic::*;

#[allow(clippy::too_many_arguments)]
pub fn read_binary<O: Offset, R: FuseReadBuf>(
    reader: &mut R,
    data_type: DataType,
    is_little_endian: bool,
    compression: Option<Compression>,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<BinaryArray<O>> {
    let validity = read_validity(reader, is_little_endian, compression, length, scratch)?;

    let offsets: Buffer<O> =
        read_buffer(reader, is_little_endian, compression, 1 + length, scratch)?;
    let last_offset = offsets.last().unwrap().to_usize();
    let values = read_buffer(reader, is_little_endian, compression, last_offset, scratch)?;

    BinaryArray::<O>::try_new(data_type, offsets, values, validity)
}
