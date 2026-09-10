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

    #[allow(dead_code)]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn user() -> DefaultUser {
        DefaultUser
    }

    async fn read_all(mut reader: Box<dyn AsyncRead + Send + Sync + Unpin>) -> Vec<u8> {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        buf
    }

    /// An `AsyncRead` that always fails, used to exercise `put`'s error path.
    struct FailingReader;

    impl AsyncRead for FailingReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::other("simulated read failure")))
        }
    }

    #[tokio::test]
    async fn metadata_reports_default_accessors() {
        let storage = MemStorage::new();
        storage
            .put(&user(), std::io::Cursor::new(b"x".to_vec()), "x.txt", 0)
            .await
            .unwrap();

        let meta = storage.metadata(&user(), "x.txt").await.unwrap();
        assert!(!meta.is_symlink());
        assert!(meta.modified().is_ok());
        assert_eq!(meta.gid(), 0);
        assert_eq!(meta.uid(), 0);
    }

    #[tokio::test]
    async fn put_propagates_a_read_error() {
        let storage = MemStorage::new();
        let err = storage.put(&user(), FailingReader, "x.txt", 0).await.err().unwrap();
        assert_eq!(err.kind(), ErrorKind::LocalError);
    }

    #[tokio::test]
    async fn metadata_of_root_is_an_empty_directory() {
        let storage = MemStorage::new();
        let meta = storage.metadata(&user(), "/").await.unwrap();
        assert!(meta.is_dir());
        assert_eq!(meta.len(), 0);
    }

    #[tokio::test]
    async fn metadata_of_unknown_file_errors() {
        let storage = MemStorage::new();
        let err = storage.metadata(&user(), "missing.txt").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermanentFileNotAvailable);
    }

    #[tokio::test]
    async fn metadata_of_known_file_reports_its_size() {
        let storage = MemStorage::new();
        storage
            .put(&user(), std::io::Cursor::new(b"hello".to_vec()), "hello.txt", 0)
            .await
            .unwrap();

        let meta = storage.metadata(&user(), "hello.txt").await.unwrap();
        assert!(meta.is_file());
        assert_eq!(meta.len(), 5);
    }

    #[tokio::test]
    async fn put_then_get_round_trips_file_content() {
        let storage = MemStorage::new();
        let size = storage
            .put(&user(), std::io::Cursor::new(b"hello world".to_vec()), "hello.txt", 0)
            .await
            .unwrap();
        assert_eq!(size, 11);

        let reader = storage.get(&user(), "hello.txt", 0).await.unwrap();
        assert_eq!(read_all(reader).await, b"hello world");
    }

    #[tokio::test]
    async fn get_honours_start_pos() {
        let storage = MemStorage::new();
        storage
            .put(&user(), std::io::Cursor::new(b"hello world".to_vec()), "hello.txt", 0)
            .await
            .unwrap();

        let reader = storage.get(&user(), "hello.txt", 6).await.unwrap();
        assert_eq!(read_all(reader).await, b"world");
    }

    #[tokio::test]
    async fn get_of_unknown_file_errors() {
        let storage = MemStorage::new();
        let err = storage.get(&user(), "missing.txt", 0).await.err().unwrap();
        assert_eq!(err.kind(), ErrorKind::PermanentFileNotAvailable);
    }

    #[tokio::test]
    async fn put_with_empty_path_errors() {
        let storage = MemStorage::new();
        let err = storage
            .put(&user(), std::io::Cursor::new(Vec::new()), "/", 0)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermanentFileNotAvailable);
    }

    #[tokio::test]
    async fn list_of_root_returns_every_file() {
        let storage = MemStorage::new();
        storage
            .put(&user(), std::io::Cursor::new(b"a".to_vec()), "a.txt", 0)
            .await
            .unwrap();
        storage
            .put(&user(), std::io::Cursor::new(b"bb".to_vec()), "b.txt", 0)
            .await
            .unwrap();

        let mut entries = storage.list(&user(), "/").await.unwrap();
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].path, PathBuf::from("a.txt"));
        assert_eq!(entries[0].metadata.len(), 1);
        assert_eq!(entries[1].path, PathBuf::from("b.txt"));
        assert_eq!(entries[1].metadata.len(), 2);
    }

    #[tokio::test]
    async fn list_of_non_root_errors() {
        let storage = MemStorage::new();
        let err = storage.list(&user(), "subdir").await.err().unwrap();
        assert_eq!(err.kind(), ErrorKind::PermanentDirectoryNotAvailable);
    }

    #[tokio::test]
    async fn del_removes_a_known_file() {
        let storage = MemStorage::new();
        storage
            .put(&user(), std::io::Cursor::new(b"a".to_vec()), "a.txt", 0)
            .await
            .unwrap();

        storage.del(&user(), "a.txt").await.unwrap();
        assert!(storage.file_names().is_empty());
    }

    #[tokio::test]
    async fn del_of_unknown_file_errors() {
        let storage = MemStorage::new();
        let err = storage.del(&user(), "missing.txt").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermanentFileNotAvailable);
    }

    #[tokio::test]
    async fn rename_moves_file_content_to_the_new_name() {
        let storage = MemStorage::new();
        storage
            .put(&user(), std::io::Cursor::new(b"hello".to_vec()), "old.txt", 0)
            .await
            .unwrap();

        storage.rename(&user(), "old.txt", "new.txt").await.unwrap();

        assert!(storage.metadata(&user(), "old.txt").await.is_err());
        let reader = storage.get(&user(), "new.txt", 0).await.unwrap();
        assert_eq!(read_all(reader).await, b"hello");
    }

    #[tokio::test]
    async fn rename_of_unknown_file_errors() {
        let storage = MemStorage::new();
        let err = storage.rename(&user(), "missing.txt", "new.txt").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermanentFileNotAvailable);
    }

    #[tokio::test]
    async fn mkd_is_not_supported() {
        let storage = MemStorage::new();
        let err = storage.mkd(&user(), "subdir").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::CommandNotImplemented);
    }

    #[tokio::test]
    async fn rmd_is_not_supported() {
        let storage = MemStorage::new();
        let err = storage.rmd(&user(), "subdir").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::CommandNotImplemented);
    }

    #[tokio::test]
    async fn cwd_to_root_succeeds() {
        let storage = MemStorage::new();
        storage.cwd(&user(), "/").await.unwrap();
    }

    #[tokio::test]
    async fn cwd_to_subdirectory_errors() {
        let storage = MemStorage::new();
        let err = storage.cwd(&user(), "subdir").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermanentDirectoryNotAvailable);
    }

    #[tokio::test]
    async fn sessions_get_independent_storage() {
        let a = MemStorage::new();
        let b = MemStorage::new();
        a.put(&user(), std::io::Cursor::new(b"only in a".to_vec()), "f.txt", 0)
            .await
            .unwrap();

        assert!(b.metadata(&user(), "f.txt").await.is_err());
    }
}
