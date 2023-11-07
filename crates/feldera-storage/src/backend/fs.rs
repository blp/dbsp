use std::collections::VecDeque;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use rkyv::AlignedVec;
use crate::backend::StorageBackend;

pub(crate) struct PosixBackend {
    outstanding: VecDeque<(AlignedVec, u64)>
}

impl StorageBackend for PosixBackend {
    fn submit_write(
        &mut self,
        fd: &File,
        offset: u64,
        buf: AlignedVec,
        reply_with: u64,
    ) -> io::Result<()>
    {
        fd.write_all_at(&buf, offset)?;
        self.outstanding.push_back((buf, reply_with));
        Ok(())
    }

    fn submit_read(
        &mut self,
        fd: &File,
        offset: u64,
        mut buf: AlignedVec,
        reply_with: u64,
    ) -> io::Result<()> {
        buf.resize(buf.capacity(), 0);

        // read from fd at offset into buf using tokio
        fd.read_exact_at(&mut buf, offset)?;
        self.outstanding.push_back((buf, reply_with));

        Ok(())
    }

    fn await_results(&mut self, how_many: usize) -> io::Result<Vec<AlignedVec>> {
        let mut returns = Vec::with_capacity(how_many);
        self.outstanding.drain(..how_many).for_each(|(buf, _)| returns.push(buf));
        Ok(returns)
    }
}

impl PosixBackend {
    pub(crate) fn new() -> Self {
        Self {
            outstanding: VecDeque::with_capacity(8)
        }
    }
}