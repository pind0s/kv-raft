use crate::error::Error;
use crate::error::Result;
use crate::pages::PageId;
use std::fs::OpenOptions;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

pub(crate) struct FileHandler {
    file: std::fs::File,
}

impl FileHandler {
    pub(crate) fn open_existing(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        Ok(FileHandler { file })
    }

    pub(crate) fn open_new(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        Ok(FileHandler { file })
    }

    #[cfg(unix)]
    pub(crate) fn read_at(&self, offset: u64, out: &mut [u8]) -> Result<()> {
        debug_assert!(offset <= self.file.metadata()?.len());

        self.read_exact_at(out, offset)?;
        Ok(())
    }

    #[cfg(unix)]
    pub(crate) fn write_at(&self, offset: u64, src: &[u8]) -> Result<()> {
        debug_assert!(offset <= self.file.metadata()?.len());

        self.write_all_at(src, offset)?;
        Ok(())
    }

    #[cfg(windows)]
    pub(crate) fn read_at(&self, mut offset: u64, out: &mut [u8]) -> Result<()> {
        debug_assert!(offset <= self.file.metadata()?.len());

        let mut read_offset = 0;
        while read_offset < out.len() {
            let read_bytes = self.file.seek_read(&mut out[read_offset..], offset)?;
            if read_bytes == 0 {
                return Err(Error::IoError(std::io::Error::from(
                    std::io::ErrorKind::UnexpectedEof,
                )));
            }
            offset += read_bytes as u64;
            read_offset += read_bytes;
        }
        Ok(())
    }

    #[cfg(windows)]
    pub(crate) fn write_at(&self, offset: u64, src: &[u8]) -> Result<()> {
        debug_assert!(offset <= self.file.metadata()?.len());

        let byte_written = self.file.seek_write(src, offset)?;

        if byte_written != src.len() {
            return Err(Error::DatabaseError(
                "Failed to write the expected number of bytes to the file".to_string(),
            ));
        }

        Ok(())
    }

    pub(crate) fn write_at_page(&self, page_id: PageId, page_size: u64, src: &[u8]) -> Result<()> {
        self.write_at(page_id * page_size, src)
    }

    pub(crate) fn sync_data(&self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    pub(crate) fn preallocate_pages(&self, additional_pages: u64, page_size: u64) -> Result<u64> {
        let new_file_len = self.file_len()? + page_size * additional_pages;
        self.file.set_len(new_file_len)?;
        Ok(new_file_len / page_size)
    }

    pub(crate) fn file_len(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }
}
