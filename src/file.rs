use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::{fs, vec};

use crate::entry::{Entry, HEADER_SIZE};
use crate::error::Error;
use crate::Result;

pub(crate) const FILE_NAME: &str = "minidb.data";
pub(crate) const MERGE_FILE_NAME: &str = "minidb.data.merge";

pub(crate) struct DBFile {
    pub(crate) file: Option<fs::File>,
    pub(crate) offset: u64,
    pub(crate) path: PathBuf,
}

impl DBFile {
    fn new_internal(filename: PathBuf) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&filename)?;
        let offset = file.metadata()?.len();
        Ok(Self {
            file: Some(file),
            offset,
            path: filename,
        })
    }

    pub(crate) fn new<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let mut path_buf = PathBuf::new();
        path_buf.push(path);
        Self::new_internal(path_buf.join(FILE_NAME))
    }

    pub(crate) fn new_merge<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let mut path_buf = PathBuf::new();
        path_buf.push(path);

        Self::new_internal(path_buf.join(MERGE_FILE_NAME))
    }

    #[inline]
    fn get_file_mut(&mut self) -> Result<&mut File> {
        self.file
            .as_mut()
            .map_or_else(|| Err(Error::DBFileNotExist), Ok)
    }
    #[inline]
    fn read_at(&mut self, buf: &mut [u8], offset: u64) -> Result<()> {
        if offset >= self.offset {
            return Err(Error::InvalidOffset);
        }

        self.get_file_mut()?.read_at(buf, offset)?;
        Ok(())
    }

    pub(crate) fn read(&mut self, offset: u64) -> Result<Entry> {
        let mut byte_mut = vec![0u8; HEADER_SIZE as usize];
        let _file = self.get_file_mut()?;
        self.read_at(&mut byte_mut, offset)?;
        // read key_size, value_size, mark
        let mut entry = Entry::decode(&byte_mut)?;

        // read key
        let offset = offset + HEADER_SIZE;
        if entry.key_size > 0 {
            let mut key = vec![0u8; entry.key_size as usize];
            self.read_at(&mut key, offset)?;
            entry.key = key.into();
        }

        // read value
        let offset = offset + entry.key_size;
        if entry.value_size > 0 {
            let mut value = vec![0u8; entry.value_size as usize];
            self.read_at(&mut value, offset)?;
            entry.value = value.into();
        }

        Ok(entry)
    }

    pub(crate) fn write(&mut self, entry: &Entry) -> Result<()> {
        let b = entry.encode()?;
        let offset = self.offset;
        let file = self.get_file_mut()?;
        file.write_at(&b, offset)?;
        file.flush()?;
        self.offset += entry.get_size();
        Ok(())
    }

    pub(crate) fn remove(&mut self) -> Result<()> {
        if let Some(file) = self.file.take() {
            drop(file);
        }
        fs::remove_file(&self.path)?;
        Ok(())
    }
}
