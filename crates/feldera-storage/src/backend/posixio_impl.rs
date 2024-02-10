//! Implementation of the storage backend APIs ([`StorageControl`],
//! [`StorageRead`], and [`StorageWrite`]) using POSIX I/O.

use futures::{task::noop_waker, Future};
use metrics::{counter, histogram};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Error as IoError, Seek},
    os::unix::prelude::FileExt,
    path::{Path, PathBuf},
    rc::Rc,
    sync::{Arc, RwLock},
    task::Context,
    time::Instant,
};
use tempfile::TempDir;
use uuid::Uuid;

use crate::{backend::NEXT_FILE_HANDLE, buffer_cache::FBuf, init};

use super::{
    metrics::{
        describe_disk_metrics, FILES_CREATED, FILES_DELETED, READS_FAILED, READS_SUCCESS,
        READ_LATENCY, TOTAL_BYTES_READ, TOTAL_BYTES_WRITTEN, WRITES_SUCCESS, WRITE_LATENCY,
    },
    AtomicIncrementOnlyI64, FileHandle, ImmutableFileHandle, StorageControl, StorageError,
    StorageExecutor, StorageRead, StorageWrite,
};

/// Helper function that opens files as direct IO files on linux.
fn open_as_direct<P: AsRef<Path>>(p: P, options: &mut OpenOptions) -> Result<File, IoError> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_DIRECT);
    }
    options.open(p)
}
/// Meta-data we keep per file we created.
struct FileMetaData {
    file: File,
    path: PathBuf,
}

/// State of the backend needed to satisfy the storage APIs.
pub struct PosixBackend {
    /// Directory in which we keep the files.
    base: PathBuf,
    /// Meta-data of all files we created so far.
    files: RwLock<HashMap<i64, FileMetaData>>,
    /// A global counter to get unique identifiers for file-handles.
    next_file_id: Arc<AtomicIncrementOnlyI64>,
}

impl PosixBackend {
    /// Instantiates a new backend.
    ///
    /// ## Parameters
    /// - `base`: Directory in which we keep the files.
    /// - `next_file_id`: A counter to get unique identifiers for file-handles.
    ///   Note that in case we use a global buffer cache, this counter should be
    ///   shared among all instances of the backend.
    pub fn new<P: AsRef<Path>>(base: P, next_file_id: Arc<AtomicIncrementOnlyI64>) -> Self {
        init();
        describe_disk_metrics();
        Self {
            base: base.as_ref().to_path_buf(),
            files: RwLock::new(HashMap::new()),
            next_file_id,
        }
    }

    /// Helper function to delete (mutable and immutable) files.
    fn delete_inner(&self, fd: i64) -> Result<(), StorageError> {
        let FileMetaData { file: _, path } = self.files.write().unwrap().remove(&fd).unwrap();
        std::fs::remove_file(path).unwrap();
        Ok(())
    }

    /// Returns the directory in which the backend creates files.
    pub fn path(&self) -> &Path {
        self.base.as_path()
    }

    /// Returns a thread-local default backend.
    pub fn default_for_thread() -> Rc<Self> {
        thread_local! {
            pub static TEMPDIR: TempDir = tempfile::tempdir().unwrap();
            pub static DEFAULT_BACKEND: Rc<PosixBackend> = {
                let path = TEMPDIR.with(|dir| dir.path().to_path_buf());
                 Rc::new(PosixBackend::new(&path, NEXT_FILE_HANDLE.get_or_init(|| {
                    Arc::new(Default::default())
                }).clone()))
            };
        }
        DEFAULT_BACKEND.with(|rc| rc.clone())
    }
}

impl StorageControl for PosixBackend {
    async fn create(&self) -> Result<FileHandle, StorageError> {
        let file_counter = self.next_file_id.increment();
        let name = Uuid::now_v7();
        let path = self.base.join(name.to_string() + ".feldera");
        let file = open_as_direct(
            &path,
            OpenOptions::new().create_new(true).write(true).read(true),
        )?;
        let mut files = self.files.write().unwrap();
        files.insert(file_counter, FileMetaData { file, path });
        counter!(FILES_CREATED).increment(1);

        Ok(FileHandle(file_counter))
    }

    async fn delete(&self, fd: ImmutableFileHandle) -> Result<(), StorageError> {
        self.delete_inner(fd.0)
            .map(|_| counter!(FILES_DELETED).increment(1))
    }

    async fn delete_mut(&self, fd: FileHandle) -> Result<(), StorageError> {
        self.delete_inner(fd.0)
            .map(|_| counter!(FILES_DELETED).increment(1))
    }
}

impl StorageWrite for PosixBackend {
    async fn write_block(
        &self,
        fd: &FileHandle,
        offset: u64,
        data: FBuf,
    ) -> Result<Arc<FBuf>, StorageError> {
        let files = self.files.read().unwrap();
        let request_start = Instant::now();
        let fm = files.get(&fd.0).unwrap();
        fm.file.write_all_at(&data, offset)?;

        counter!(TOTAL_BYTES_WRITTEN).increment(data.len() as u64);
        counter!(WRITES_SUCCESS).increment(1);
        histogram!(WRITE_LATENCY).record(request_start.elapsed().as_secs_f64());

        Ok(Arc::new(data))
    }

    async fn complete(&self, fd: FileHandle) -> Result<ImmutableFileHandle, StorageError> {
        let mut files = self.files.write().unwrap();

        let fm = files.remove(&fd.0).unwrap();
        //fm.file.sync_all();
        files.insert(fd.0, fm);

        Ok(ImmutableFileHandle(fd.0))
    }
}

impl StorageRead for PosixBackend {
    async fn prefetch(&self, _fd: &ImmutableFileHandle, _offset: u64, _size: usize) {
        unimplemented!()
    }

    async fn read_block(
        &self,
        fd: &ImmutableFileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Arc<FBuf>, StorageError> {
        let mut buffer = FBuf::with_capacity(size);
        buffer.resize(size, 0);

        let files = self.files.read().unwrap();
        let fm = files.get(&fd.0).unwrap();
        let request_start = Instant::now();
        match fm.file.read_exact_at(&mut buffer[..], offset) {
            Ok(()) => {
                counter!(TOTAL_BYTES_READ).increment(buffer.len() as u64);
                histogram!(READ_LATENCY).record(request_start.elapsed().as_secs_f64());
                counter!(READS_SUCCESS).increment(1);
                Ok(Arc::new(buffer))
            }
            Err(e) => {
                counter!(READS_FAILED).increment(1);
                Err(e.into())
            }
        }
    }

    async fn get_size(&self, fd: &ImmutableFileHandle) -> Result<u64, StorageError> {
        let mut files = self.files.write().unwrap();
        let fm = files.get_mut(&fd.0).unwrap();
        let size = fm.file.seek(std::io::SeekFrom::End(0))?;
        Ok(size)
    }
}

impl StorageExecutor for PosixBackend {
    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        // Extracts the result from `future` assuming that it's already ready.
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut pinned = std::pin::pin!(future);
        match pinned.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(output) => output,
            std::task::Poll::Pending => unreachable!(),
        }
    }
}
