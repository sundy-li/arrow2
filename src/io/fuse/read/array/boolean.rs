use std::io::{BufReader, Read};

use crate::array::BooleanArray;
use crate::datatypes::DataType;
use crate::error::Result;
use crate::io::fuse::read::Compression;

use super::super::read_basic::*;

#[allow(clippy::too_many_arguments)]
pub fn read_boolean<R: Read>(
    reader: &mut BufReader<R>,
    data_type: DataType,
    is_little_endian: bool,
    compression: Option<Compression>,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<BooleanArray> {
    let validity = read_validity(reader, is_little_endian, compression, length, scratch)?;
    let values = read_bitmap(reader, compression, length, scratch)?;
    BooleanArray::try_new(data_type, values, validity)
}
