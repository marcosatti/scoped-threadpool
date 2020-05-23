#![feature(thread_spawn_unchecked)]

use crossbeam::{
    queue::ArrayQueue,
    utils::Backoff,
};
use std::{
    panic::{
        catch_unwind,
        AssertUnwindSafe,
    },
    sync::{
        atomic::{
            AtomicBool,
            Ordering,
        },
        Arc,
    },
    thread,
    thread::Result as ThreadResult,
};

pub trait Thunk {
    fn call_once(self);
}

pub struct ThreadPool<F>
where F: Thunk + Send
{
    pool: Vec<thread::JoinHandle<()>>,
    data: Arc<Data<F>>,
}

impl<F> ThreadPool<F>
where F: Thunk + Send
{
    pub fn new(pool_size: usize, queue_size: usize, thread_name_prefix: &'static str) -> ThreadPool<F> {
        let mut pool = Vec::new();
        let data = Arc::new(Data::new(queue_size));

        for i in 0..pool_size {
            let data = data.clone();
            let name = format!("{}-{}", thread_name_prefix, i);
            let handle = unsafe { thread::Builder::new().name(name).spawn_unchecked(move || thread_main(data)).unwrap() };
            pool.push(handle);
        }

        ThreadPool {
            pool,
            data,
        }
    }

    pub fn scope<'a, F2, F3, S>(&self, this_thread_fn: Option<F2>, scope_fn: S)
    where
        F2: Thunk + 'a,
        F3: Thunk + Send + 'a,
        S: FnOnce(&mut Scope<'a, F3>),
    {
        let send_queue = unsafe { std::mem::transmute(&self.data.send_queue) };

        let mut scope = Scope::new(send_queue);
        scope_fn(&mut scope);

        if this_thread_fn.is_some() {
            this_thread_fn.unwrap().call_once();
        }

        let backoff = Backoff::new();
        let target_count = scope.consume();
        let mut count = 0;
        while count < target_count {
            match self.data.recv_queue.pop() {
                Ok(r) => {
                    r.unwrap();
                    count += 1;
                    backoff.reset();
                },
                Err(_) => {
                    backoff.spin();
                },
            }
        }
    }
}

impl<F> Drop for ThreadPool<F>
where F: Thunk + Send
{
    fn drop(&mut self) {
        self.data.stop.store(true, Ordering::SeqCst);
        self.pool.drain(..).for_each(|h| h.join().unwrap());
    }
}

struct Data<F>
where F: Thunk + Send
{
    send_queue: ArrayQueue<F>,
    recv_queue: ArrayQueue<ThreadResult<()>>,
    stop: AtomicBool,
}

impl<F> Data<F>
where F: Thunk + Send
{
    fn new(queue_size: usize) -> Data<F> {
        Data {
            send_queue: ArrayQueue::new(queue_size),
            recv_queue: ArrayQueue::new(queue_size),
            stop: AtomicBool::new(false),
        }
    }
}

fn thread_main<F>(data: Arc<Data<F>>)
where F: Thunk + Send {
    let backoff = Backoff::new();

    while !data.stop.load(Ordering::Relaxed) {
        match data.send_queue.pop() {
            Ok(thunk) => {
                let result = catch_unwind(AssertUnwindSafe(move || thunk.call_once()));
                match data.recv_queue.push(result) {
                    Ok(()) => backoff.reset(),
                    Err(_) => panic!("Error pushing to result queue"),
                }
            },
            Err(_) => {
                backoff.spin();
            },
        }
    }
}

pub struct Scope<'a, F>
where F: Thunk + Send
{
    send_queue: &'a ArrayQueue<F>,
    counter: usize,
}

impl<'a, F> Scope<'a, F>
where F: Thunk + Send
{
    fn new(send_queue: &'a ArrayQueue<F>) -> Scope<'a, F> {
        Scope {
            send_queue,
            counter: 0,
        }
    }

    pub fn spawn(&mut self, thunk: F) {
        self.send_queue.push(thunk).unwrap();
        self.counter += 1;
    }

    fn consume(self) -> usize {
        self.counter
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Task<'a>(&'a mut i32);

    impl<'a> Thunk for Task<'a> {
        fn call_once(self) {
            *self.0 = 2;
        }
    }

    #[test]
    fn simple_test() {
        let pool = ThreadPool::<Task>::new(2, 16, "test");

        {
            let mut number = 1;

            pool.scope::<Task, Task, _>(None, |s| {
                s.spawn(Task {
                    0: &mut number,
                });
            });

            assert_eq!(number, 2);
        }
    }
}
