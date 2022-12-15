use crate::error::Result;
use std::io::Read;
use std::io::Write;

#[cfg(feature = "io_fuse")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_fuse")))]
pub fn decompress_lz4<R: Read>(input_buf: R, output_buf: &mut [u8]) -> Result<()> {
    let mut decoder = lz4::Decoder::new(input_buf)?;
    decoder.read_exact(output_buf).unwrap();
    let (mut r, _) = decoder.finish();
    r.read_to_end(&mut vec![])?;
    Ok(())
}

#[cfg(feature = "io_fuse")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_fuse")))]
pub fn decompress_zstd<R: Read>(input_buf: R, output_buf: &mut [u8]) -> Result<()> {
    let mut decoder = zstd::Decoder::new(input_buf)?;
    decoder.read_exact(output_buf).map_err(|e| e.into())
}

#[cfg(not(feature = "io_fuse"))]
pub fn decompress_lz4<R: Read>(_input_buf: R, _output_buf: &mut [u8]) -> Result<()> {
    use crate::error::Error;
    Err(Error::OutOfSpec(
        "The crate was compiled without IPC compression. Use `io_fuse` to read compressed IPC."
            .to_string(),
    ))
}

#[cfg(not(feature = "io_fuse"))]
pub fn decompress_zstd<R: Read>(_input_buf: R, _output_buf: &mut [u8]) -> Result<()> {
    use crate::error::Error;
    Err(Error::OutOfSpec(
        "The crate was compiled without IPC compression. Use `io_fuse` to read compressed IPC."
            .to_string(),
    ))
}

#[cfg(feature = "io_fuse")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_fuse")))]
pub fn compress_lz4<W: Write>(input_buf: &[u8], output_buf: &mut W) -> Result<()> {
    use crate::error::Error;
    let mut encoder = lz4::EncoderBuilder::new()
        .build(output_buf)
        .map_err(Error::from)?;
    encoder.write_all(input_buf)?;
    encoder.finish().1.map_err(|e| e.into())
}

#[cfg(feature = "io_fuse")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_fuse")))]
pub fn compress_zstd<W: Write>(input_buf: &[u8], output_buf: &mut W) -> Result<()> {
    zstd::stream::copy_encode(input_buf, output_buf, 0).map_err(|e| e.into())
}

#[cfg(not(feature = "io_fuse"))]
pub fn compress_lz4<W: Write>(_input_buf: &[u8], _output_buf: &mut W) -> Result<()> {
    use crate::error::Error;
    Err(Error::OutOfSpec(
        "The crate was compiled without IPC compression. Use `io_fuse` to write compressed IPC."
            .to_string(),
    ))
}

#[cfg(not(feature = "io_fuse"))]
pub fn compress_zstd<W: Write>(_input_buf: &[u8], _output_buf: &mut W) -> Result<()> {
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
