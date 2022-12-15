
use std::io::Read;

use crate::array::BooleanArray;
use crate::datatypes::DataType;
use crate::error::{Result};
use crate::io::fuse::read::Compression;


use super::super::read_basic::*;

#[allow(clippy::too_many_arguments)]
pub fn read_boolean<R: Read>(
    reader: &mut R,
    data_type: DataType,
    is_little_endian: bool,
    compression: Option<Compression>,
    length: usize,
) -> Result<BooleanArray> {
    let validity = read_validity(reader, is_little_endian, compression, length)?;
    let values = read_bitmap(reader, compression, length)?;
    BooleanArray::try_new(data_type, values, validity)
}
