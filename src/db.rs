use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use bytes::Bytes;

use crate::entry::Entry;
use crate::error::Error;
use crate::file::DBFile;
use crate::method::Method;
use crate::Result;

struct Data {
    /// indexes in memory
    indexes: HashMap<Bytes, u64>,
    /// data file
    db_file: Option<DBFile>,
}

impl Data {
    #[inline]
    fn get_db_file_mut(&mut self) -> Result<&mut DBFile> {
        self.db_file
            .as_mut()
            .map_or_else(|| Err(Error::DBFileNotExist), Ok)
    }
}

pub struct MiniDB {
    data: Mutex<Data>,
    /// data directory
    dir_path: PathBuf,
}

impl MiniDB {
    pub fn new<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        // if directory is not exist, make directory.
        let mut dir_path = PathBuf::new();
        dir_path.push(path);
        if !dir_path.exists() {
            fs::DirBuilder::new()
                .mode(0o777)
                .recursive(true)
                .create(&dir_path)?;
        }

        let db_file = DBFile::new(&dir_path)?;
        let mut db = Self {
            dir_path,
            data: Mutex::new(Data {
                db_file: Some(db_file),
                indexes: HashMap::new(),
            }),
        };
        // load index from file
        db.load_indexes_from_file()?;

        Ok(db)
    }

    fn load_indexes_from_file(&mut self) -> Result<()> {
        let mut offset = 0u64;
        let mut db = self.data.lock()?;
        loop {
            let entry = match db.get_db_file_mut()?.read(offset) {
                Ok(e) => e,
                Err(Error::Eof) | Err(Error::EmptyContent) | Err(Error::InvalidOffset) => {
                    return Ok(());
                }
                Err(e) => return Err(e),
            };
            offset += entry.get_size();
            match entry.mark {
                Method::Del => {
                    db.indexes.remove(&entry.key);
                }
                Method::Put => {
                    db.indexes.insert(entry.key, offset);
                }
            }
        }
    }

    pub fn merge(&mut self) -> Result<()> {
        let mut db = self.data.lock()?;
        if db.get_db_file_mut()?.offset == 0 {
            return Ok(());
        }

        let mut offset = 0u64;
        let mut valid_entries = vec![];

        loop {
            let e = match db.get_db_file_mut()?.read(offset) {
                Ok(entry) => entry,
                Err(Error::Eof) | Err(Error::EmptyContent) | Err(Error::InvalidOffset) => break,
                Err(a) => return Err(a),
            };
            let size = e.get_size();
            if let Some(&o) = db.indexes.get(&e.key) {
                if o == offset {
                    valid_entries.push(e);
                }
            }

            offset += size;
        }

        if valid_entries.is_empty() {
            return Ok(());
        }

        let mut merge_db_file = DBFile::new_merge(&self.dir_path)?;

        // rewrite valid entries.
        for e in valid_entries.into_iter() {
            let offset = merge_db_file.offset;
            merge_db_file.write(&e)?;
            // update index
            db.indexes.insert(e.key, offset);
        }

        // remove old db file, and rename merge db filename to old db filename
        // and set db_file to merge_db_file.
        let raw_path = db.get_db_file_mut()?.path.clone();
        db.db_file.take().map(|mut x| x.remove());
        fs::rename(&merge_db_file.path, raw_path)?;
        db.db_file.replace(merge_db_file);

        Ok(())
    }

    pub fn put(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        let mut db = self.data.lock()?;
        let offset = db.get_db_file_mut()?.offset;
        let entry = Entry::new(key, value, Method::Put);
        db.get_db_file_mut()?.write(&entry)?;
        db.indexes.insert(entry.key, offset);
        Ok(())
    }

    pub fn get(&mut self, key: Bytes) -> Result<Bytes> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        let mut db = self.data.lock()?;
        let offset = db
            .indexes
            .get(&key)
            .map_or_else(|| Err(Error::KeyNotExists), |&x| Ok(x))?;
        let entry = db.get_db_file_mut()?.read(offset)?;

        Ok(entry.value)
    }

    pub fn delete(&mut self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        let mut db = self.data.lock()?;
        let _offset = db
            .indexes
            .get(&key)
            .map_or_else(|| Err(Error::KeyNotExists), |&x| Ok(x))?;

        let entry = Entry::new(key, Bytes::new(), Method::Del);
        db.get_db_file_mut()?.write(&entry)?;

        db.indexes.remove(&entry.key);
        Ok(())
    }

    pub fn remove(&self) -> Result<()> {
        let mut db = self.data.lock()?;
        db.db_file.take().map(|mut x| x.remove());
        Ok(())
    }
}
