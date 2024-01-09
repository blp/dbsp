#![allow(unreachable_code, unused_variables, unused_mut)]
use crate::backend::StorageBackend;
use io_uring::squeue::Entry;
use io_uring::{opcode, types, IoUring};
use rkyv::AlignedVec;
use std::collections::VecDeque;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;

pub(crate) fn make_write_entry(fd: &File, offset: u64, buf: &AlignedVec) -> Entry {
    opcode::Write::new(types::Fd(fd.as_raw_fd()), buf.as_ptr(), buf.len() as _)
        .offset(offset)
        .build()
}

pub(crate) fn make_read_entry(fd: &File, offset: u64, buf: &mut AlignedVec) -> Entry {
    opcode::Read::new(
        types::Fd(fd.as_raw_fd()),
        buf.as_mut_ptr(),
        buf.capacity() as _,
    )
    .offset(offset)
    .build()
}

pub(crate) struct IoUringBackend {
    ring: IoUring,
    outstanding: VecDeque<AlignedVec>,
}

impl StorageBackend for IoUringBackend {
    fn submit_write(
        &mut self,
        fd: &File,
        offset: u64,
        buf: AlignedVec,
        reply_with: u64,
    ) -> io::Result<()> {
        println!("submit_write len buf: {:?} at offset {offset}", buf.len());
        let write_entry = make_write_entry(&fd, offset, &buf).user_data(reply_with);
        self.outstanding.push_back(buf);
        self.submit_entry(write_entry)?;
        Ok(())
    }

    fn submit_read(
        &mut self,
        fd: &File,
        offset: u64,
        mut buf: AlignedVec,
        reply_with: u64,
    ) -> io::Result<()> {
        let entry = make_read_entry(&fd, offset, &mut buf).user_data(reply_with);
        self.outstanding.push_back(buf);
        self.submit_entry(entry)?;
        Ok(())
    }

    fn await_results(&mut self, how_many: usize) -> io::Result<Vec<AlignedVec>> {
        assert_eq!(how_many, 1); // XXX

        self.ring.submit_and_wait(how_many)?;
        let mut return_buffers = Vec::with_capacity(how_many);

        for i in 0..how_many {
            let cqe = self.ring.completion().next().unwrap();
            let mut io_buf = self.outstanding.pop_front().unwrap();
            let mut to_read = 0x0;
            panic!("determine to-read here");

            // SAFETY:
            // 1. `old_len..0` is empty to no elements need to be initialized.
            //  -> we call `submit_and_wait` and `completion` so we know the kernel
            //     has written to the buffer
            //  -> we verify all bytes were written using (`cqe.result() ==
            //     to_read`)
            // 2. `0 <= capacity` always holds whatever capacity is. -> this is true
            //  because we just allocated it with capacity 24
            if cqe.result() == to_read as i32 {
                // TODO as cast, capacity should be limited to i32 with assert
                unsafe {
                    io_buf.set_len(cqe.result() as usize);
                }
            }
            return_buffers.push(io_buf);
        }
        Ok(return_buffers)
    }
}

impl IoUringBackend {
    pub(crate) fn new() -> io::Result<Self> {
        let ring = IoUring::new(8)?;
        Ok(Self {
            ring,
            outstanding: VecDeque::with_capacity(1),
        })
    }

    fn submit_entry(&mut self, entry: Entry) -> io::Result<()> {
        // TODO the unsafe here probably isn't at the right place and we can
        // make it safer
        unsafe {
            self.ring
                .submission()
                .push(&entry)
                .expect("submission queue is full");
        }
        self.ring.submit()?;
        Ok(())
    }
}
