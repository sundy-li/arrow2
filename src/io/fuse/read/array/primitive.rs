use std::convert::TryInto;
use std::io::{BufReader, Read};

use crate::datatypes::DataType;
use crate::error::Result;
use crate::io::fuse::read::read_basic::*;
use crate::io::fuse::read::Compression;
use crate::{array::PrimitiveArray, types::NativeType};

#[allow(clippy::too_many_arguments)]
pub fn read_primitive<T: NativeType, R: Read>(
    reader: &mut BufReader<R>,
    data_type: DataType,
    is_little_endian: bool,
    compression: Option<Compression>,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<PrimitiveArray<T>>
where
    Vec<u8>: TryInto<T::Bytes>,
{
    let validity = read_validity(reader, is_little_endian, compression, length, scratch)?;

    let values = read_buffer(reader, is_little_endian, compression, length, scratch)?;
    PrimitiveArray::<T>::try_new(data_type, values, validity)
}
