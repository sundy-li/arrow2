use crate::error::Result;
use std::io::Read;
use std::io::Write;

#[cfg(feature = "io_fuse")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_fuse")))]
pub fn decompress_lz4(input_buf: &[u8], output_buf: &mut [u8]) -> Result<()> {
    lz4::block::decompress_to_buffer(input_buf, Some(output_buf.len() as i32), output_buf)
        .map(|_| {})
        .map_err(|e| e.into())
}

#[cfg(feature = "io_fuse")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_fuse")))]
pub fn decompress_zstd(input_buf: &[u8], output_buf: &mut [u8]) -> Result<()> {
    zstd::bulk::decompress_to_buffer(input_buf, output_buf)
        .map(|_| {})
        .map_err(|e| e.into())
}

#[cfg(not(feature = "io_fuse"))]
pub fn decompress_lz4(_input_buf: &[u8], _output_buf: &mut [u8]) -> Result<()> {
    use crate::error::Error;
    Err(Error::OutOfSpec(
        "The crate was compiled without IPC compression. Use `io_fuse` to read compressed IPC."
            .to_string(),
    ))
}

#[cfg(not(feature = "io_fuse"))]
pub fn decompress_zstd(_input_buf: &[u8], _output_buf: &mut [u8]) -> Result<()> {
    use crate::error::Error;
    Err(Error::OutOfSpec(
        "The crate was compiled without IPC compression. Use `io_fuse` to read compressed IPC."
            .to_string(),
    ))
}

#[cfg(feature = "io_fuse")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_fuse")))]
pub fn compress_lz4(input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
    let bound = lz4::block::compress_bound(input_buf.len())?;
    output_buf.resize(bound, 0);
    lz4::block::compress_to_buffer(input_buf, None, false, output_buf.as_mut_slice())
        .map_err(|e| e.into())
}

#[cfg(feature = "io_fuse")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_fuse")))]
pub fn compress_zstd(input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
    let bound = zstd::zstd_safe::compress_bound(input_buf.len());
    output_buf.resize(bound, 0);
    zstd::bulk::compress_to_buffer(input_buf, output_buf.as_mut_slice(), 0).map_err(|e| e.into())
}

#[cfg(not(feature = "io_fuse"))]
pub fn compress_lz4(_input_buf: &[u8], _output_buf: &mut Vec<u8>) -> Result<usize> {
    use crate::error::Error;
    Err(Error::OutOfSpec(
        "The crate was compiled without IPC compression. Use `io_fuse` to write compressed IPC."
            .to_string(),
    ))
}

#[cfg(not(feature = "io_fuse"))]
pub fn compress_zstd(_input_buf: &[u8], _output_buf: &mut Vec<u8>) -> Result<usize> {
    use crate::error::Error;
    Err(Error::OutOfSpec(
        "The crate was compiled without IPC compression. Use `io_fuse` to write compressed IPC."
            .to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "io_fuse")]
    #[test]
    #[cfg_attr(miri, ignore)] // ZSTD uses foreign calls that miri does not support
    fn round_trip_zstd() {
        let data: Vec<u8> = (0..200u8).map(|x| x % 10).collect();
        let mut buffer = vec![];
        compress_zstd(&data, &mut buffer).unwrap();

        let mut result = vec![0; 200];
        decompress_zstd(&buffer, &mut result).unwrap();
        assert_eq!(data, result);
    }

    #[cfg(feature = "io_fuse")]
    #[test]
    #[cfg_attr(miri, ignore)] // LZ4 uses foreign calls that miri does not support
    fn round_trip_lz4() {
        let data: Vec<u8> = (0..200u8).map(|x| x % 10).collect();
        let mut buffer = vec![];
        compress_lz4(&data, &mut buffer).unwrap();

        let mut result = vec![0; 200];
        decompress_lz4(&buffer, &mut result).unwrap();
        assert_eq!(data, result);
    }
}
