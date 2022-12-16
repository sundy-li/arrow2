use std::io::Write;

use super::common_sync::write_continuation;
use super::{super::ARROW_MAGIC, common::WriteOptions};

use crate::chunk::Chunk;
use crate::datatypes::*;
use crate::error::{Error, Result};
use crate::{array::Array, io::fuse::ColumnMeta};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    None,
    Started,
    Written,
    Finished,
}

/// Arrow file writer
pub struct FuseWriter<W: Write> {
    /// The object to write to
    pub(crate) writer: OffsetWriter<W>,
    /// fuse write options
    pub(crate) options: WriteOptions,
    /// A reference to the schema, used in validating record batches
    pub(crate) schema: Schema,

    /// Record blocks that will be written as part of the fuse footer
    pub metas: Vec<ColumnMeta>,

    pub(crate) scratch: Vec<u8>,
    /// Whether the writer footer has been written, and the writer is finished
    pub(crate) state: State,
}

impl<W: Write> FuseWriter<W> {
    /// Creates a new [`FuseWriter`] and writes the header to `writer`
    pub fn try_new(writer: W, schema: &Schema, options: WriteOptions) -> Result<Self> {
        let mut slf = Self::new(writer, schema.clone(), options);
        slf.start()?;

        Ok(slf)
    }

    /// Creates a new [`FuseWriter`].
    pub fn new(writer: W, schema: Schema, options: WriteOptions) -> Self {
        let num_cols = schema.fields.len();
        Self {
            writer: OffsetWriter {
                w: writer,
                offset: 0,
            },
            options,
            schema,
            metas: Vec::with_capacity(num_cols),
            scratch: Vec::with_capacity(0),
            state: State::None,
        }
    }

    /// Consumes itself into the inner writer
    pub fn into_inner(self) -> W {
        self.writer.w
    }

    /// Writes the header and first (schema) message to the file.
    /// # Errors
    /// Errors if the file has been started or has finished.
    pub fn start(&mut self) -> Result<()> {
        if self.state != State::None {
            return Err(Error::oos("The fuse file can only be started once"));
        }
        // write magic to header
        self.writer.write_all(&ARROW_MAGIC[..])?;
        // create an 8-byte boundary after the header
        self.writer.write_all(&[0, 0])?;

        // write the schema, set the written bytes to the schema
        self.state = State::Started;
        Ok(())
    }

    /// Writes [`Chunk`] to the file
    pub fn write(&mut self, chunk: &Chunk<Box<dyn Array>>) -> Result<()> {
        if self.state != State::Started {
            return Err(Error::oos(
                "The fuse file must be started before it can be written to. Call `start` before `write`",
            ));
        }
        assert_eq!(chunk.arrays().len(), self.schema.fields.len());
        self.encode_chunk(chunk)?;

        self.state = State::Written;
        Ok(())
    }

    /// Write footer and closing tag, then mark the writer as done
    pub fn finish(&mut self) -> Result<()> {
        if self.state != State::Written {
            return Err(Error::oos(
                "The fuse file must be written before it can be finished. Call `start` before `finish`",
            ));
        }
        // write EOS
        write_continuation(&mut self.writer, 0)?;
        self.writer.write_all(&ARROW_MAGIC)?;
        self.writer.flush()?;
        self.state = State::Finished;
        Ok(())
    }

    pub fn total_size(&self) -> usize {
        self.writer.offset()
    }
}

pub struct OffsetWriter<W: Write> {
    pub w: W,
    pub offset: u64,
}

impl<W: Write> std::io::Write for OffsetWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let size = self.w.write(buf)?;
        self.offset += size as u64;
        Ok(size)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.w.flush()
    }
}

pub trait OffsetWrite: std::io::Write {
    fn offset(&self) -> usize;
}

impl<W: std::io::Write> OffsetWrite for OffsetWriter<W> {
    fn offset(&self) -> usize {
        self.offset as usize
    }
}
