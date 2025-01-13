// use std::collections::VecDeque;
// use std::io::Error;
// use std::sync::atomic::AtomicU64;
// use std::sync::{Arc, Mutex};
// use std::thread;
// use std::thread::Thread;
// use crossbeam::atomic::AtomicCell;
// use crate::write_batch::WriteBatch;
// use crate::write_ahead_log::WriteAheadLog;

// struct WritesManager {
//
//     queue: Mutex<VecDeque<Writer>>,
//     write_ahead_log: Mutex<WriteAheadLog>,
//     next_sequence_number: AtomicU64,
// }

// impl WritesManager {
//
//     fn write(&self, batch: WriteBatch) -> Result<(), Error>{
//
//         let writer = Writer::new(batch);
//
//         let writer_ref: &Writer = &writer;
//
//         // Add the writer to the queue
//         self.queue.lock().unwrap().push_back(writer);
//
//         if self.is_leader(writer_ref) {
//             self.perform_writes();
//         } else {
//             while !writer_ref.is_done() {
//                 thread::park();
//             }
//         }
//         writer_ref.result()
//     }
//
//     fn is_leader(&self, writer_ref: &Writer) -> bool {
//         self.queue
//             .lock().unwrap()
//             .front()
//             .map_or(false, |front| std::ptr::eq(front, writer_ref))
//     }
//
//     fn perform_writes(&self) {
//         let mut queue = self.queue.lock().unwrap();
//
//         let mut writes = Vec::new();
//         while let Some(writer) = queue.pop_front() {
//             writes.push(writer);
//         }
//
//         let mut wal = self.write_ahead_log.lock().unwrap();
//
//         drop(queue);
//
//         for mut writer in writes {
//             let mut write_batch = writer.write_batch;
//             write_batch.sequence_number = self.next_sequence_number.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
//
//             let res = wal.log(write_batch);
//             writer.writer_thread.unpark();
//         }
//     }
// }
//
// struct Writer {
//     writer_thread: Thread,
//     write_batch: WriteBatch,
//     may_be_result: AtomicCell<Option<Result<(), Arc<Error>>>>,
// }
//
// impl Writer {
//
//     fn new(batch: WriteBatch) -> Writer {
//         Writer {
//             writer_thread: thread::current(),
//             write_batch: batch,
//             may_be_result: AtomicCell::new(None),
//         }
//     }
//
//     fn result(&self) -> Result<(), Error> {
//         match self.may_be_result.load() {
//             Ok(_) => Ok(()),
//             Err(arc) => Err(arc.),
//         }
//
//         result.expect("The writer thread was unexpectedly terminated.")
//     }
//
//     fn is_done(&self) -> bool {
//         None != self.may_be_result.load()
//     }
//
//     fn done(&self, result : Result<(), Error>) {
//         self.may_be_result.store(Some(result));
//         self.writer_thread.unpark();
//     }
// }

