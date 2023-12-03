use rkyv::AlignedVec;
use std::fs::File;
use std::io;

pub(crate) mod fs;
#[cfg(target_os = "linux")]
mod uring;

pub(crate) mod metadata;

pub(crate) const FILE_VERSION_FORMAT: u32 = 0x01;

pub enum Backend {
    #[cfg(target_os = "linux")]
    IoUring,
    Posix,
}

impl Backend {
    pub(crate) fn create(&self) -> Box<dyn StorageBackend> {
        match self {
            #[cfg(target_os = "linux")]
            Backend::IoUring => Box::new(uring::IoUringBackend::new().unwrap()),
            Backend::Posix => Box::new(fs::PosixBackend::new()),
        }
    }
}

pub trait StorageBackend {
    fn submit_write(
        &mut self,
        fd: &File,
        offset: u64,
        ds: AlignedVec,
        reply_with: u64,
    ) -> io::Result<()>;

    fn submit_read(
        &mut self,
        fd: &File,
        offset: u64,
        buf: AlignedVec,
        reply_with: u64,
    ) -> io::Result<()>;

    fn await_results(&mut self, how_many: usize) -> io::Result<Vec<AlignedVec>>;
}
