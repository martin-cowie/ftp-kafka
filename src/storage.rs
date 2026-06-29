use async_trait::async_trait;
use std::{
    collections::HashMap,
    fmt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::SystemTime,
};
use tokio::io::{AsyncRead, AsyncReadExt};
use unftp_core::{
    auth::DefaultUser,
    storage::{Error, ErrorKind, Fileinfo, Metadata, StorageBackend},
};

#[derive(Debug, Clone)]
pub struct MemMetadata {
    pub size: u64,
    pub is_dir: bool,
    pub modified: SystemTime,
}

impl Metadata for MemMetadata {
    fn len(&self) -> u64 {
        self.size
    }
    fn is_dir(&self) -> bool {
        self.is_dir
    }
    fn is_file(&self) -> bool {
        !self.is_dir
    }
    fn is_symlink(&self) -> bool {
        false
    }
    fn modified(&self) -> Result<SystemTime, Error> {
        Ok(self.modified)
    }
    fn gid(&self) -> u32 {
        0
    }
    fn uid(&self) -> u32 {
        0
    }
}

#[derive(Debug)]
pub struct MemStorage {
    files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl MemStorage {
    pub fn new() -> Self {
        MemStorage {
            files: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Remove any `/` prefix
    fn normalize(path: &Path) -> String {
        let s = path.to_string_lossy();
        let s = s.trim_start_matches('/');
        s.to_string()
    }

    pub fn file_names(&self) -> Vec<String> {
        self.files.lock().unwrap().keys().cloned().collect()
    }
}

#[async_trait]
impl StorageBackend<DefaultUser> for MemStorage {
    type Metadata = MemMetadata;

    async fn metadata<P: AsRef<Path> + Send + fmt::Debug>(
        &self,
        _user: &DefaultUser,
        path: P,
    ) -> Result<MemMetadata, Error> {
        let key = Self::normalize(path.as_ref());
        if key.is_empty() {
            return Ok(MemMetadata {
                size: 0,
                is_dir: true,
                modified: SystemTime::now(),
            });
        }
        let files = self.files.lock().unwrap();
        match files.get(&key) {
            Some(data) => Ok(MemMetadata {
                size: data.len() as u64,
                is_dir: false,
                modified: SystemTime::now(),
            }),
            None => Err(Error::new(
                ErrorKind::PermanentFileNotAvailable,
                "File not found",
            )),
        }
    }

    async fn list<P: AsRef<Path> + Send + fmt::Debug>(
        &self,
        _user: &DefaultUser,
        path: P,
    ) -> Result<Vec<Fileinfo<PathBuf, MemMetadata>>, Error> {
        let key = Self::normalize(path.as_ref());
        if !key.is_empty() {
            return Err(Error::new(
                ErrorKind::PermanentDirectoryNotAvailable,
                "Not a directory",
            ));
        }
        let files = self.files.lock().unwrap();
        let entries = files
            .iter()
            .map(|(name, data)| Fileinfo {
                path: PathBuf::from(name),
                metadata: MemMetadata {
                    size: data.len() as u64,
                    is_dir: false,
                    modified: SystemTime::now(),
                },
            })
            .collect();
        Ok(entries)
    }

    async fn get<P: AsRef<Path> + Send + fmt::Debug>(
        &self,
        _user: &DefaultUser,
        path: P,
        start_pos: u64,
    ) -> Result<Box<dyn AsyncRead + Send + Sync + Unpin>, Error> {
        let path = Self::normalize(path.as_ref());
        let files = self.files.lock().unwrap();
        match files.get(&path) {
            Some(data) => {
                let start = (start_pos as usize).min(data.len());
                Ok(Box::new(std::io::Cursor::new(data[start..].to_vec())))
            }
            None => Err(Error::new(
                ErrorKind::PermanentFileNotAvailable,
                "File not found",
            )),
        }
    }

    async fn put<P, R>(
        &self,
        _user: &DefaultUser,
        mut input: R,
        path: P,
        _start_pos: u64,
    ) -> Result<u64, Error>
    where
        P: AsRef<Path> + Send + fmt::Debug,
        R: AsyncRead + Send + Sync + Unpin + 'static,
    {
        let key = Self::normalize(path.as_ref());
        if key.is_empty() {
            return Err(Error::new(
                ErrorKind::PermanentFileNotAvailable,
                "Invalid path",
            ));
        }
        let mut buf = Vec::new();
        input
            .read_to_end(&mut buf)
            .await
            .map_err(|e| Error::new(ErrorKind::LocalError, e.to_string()))?;
        let size = buf.len() as u64;
        self.files.lock().unwrap().insert(key, buf);
        Ok(size)
    }

    async fn del<P: AsRef<Path> + Send + fmt::Debug>(
        &self,
        _user: &DefaultUser,
        path: P,
    ) -> Result<(), Error> {
        let key = Self::normalize(path.as_ref());
        let mut files = self.files.lock().unwrap();
        if files.remove(&key).is_some() {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::PermanentFileNotAvailable,
                "File not found",
            ))
        }
    }

    async fn mkd<P: AsRef<Path> + Send + fmt::Debug>(
        &self,
        _user: &DefaultUser,
        _path: P,
    ) -> Result<(), Error> {
        Err(Error::new(
            ErrorKind::CommandNotImplemented,
            "Directories not supported",
        ))
    }

    async fn rename<P: AsRef<Path> + Send + fmt::Debug>(
        &self,
        _user: &DefaultUser,
        from: P,
        to: P,
    ) -> Result<(), Error> {
        let from_key = Self::normalize(from.as_ref());
        let to_key = Self::normalize(to.as_ref());
        let mut files = self.files.lock().unwrap();
        match files.remove(&from_key) {
            Some(data) => {
                files.insert(to_key, data);
                Ok(())
            }
            None => Err(Error::new(
                ErrorKind::PermanentFileNotAvailable,
                "File not found",
            )),
        }
    }

    async fn rmd<P: AsRef<Path> + Send + fmt::Debug>(
        &self,
        _user: &DefaultUser,
        _path: P,
    ) -> Result<(), Error> {
        Err(Error::new(
            ErrorKind::CommandNotImplemented,
            "Directories not supported",
        ))
    }

    async fn cwd<P: AsRef<Path> + Send + fmt::Debug>(
        &self,
        _user: &DefaultUser,
        path: P,
    ) -> Result<(), Error> {
        let key = Self::normalize(path.as_ref());
        if key.is_empty() {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::PermanentDirectoryNotAvailable,
                "Only root directory supported",
            ))
        }
    }
}
