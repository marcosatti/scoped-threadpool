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
    time::Duration,
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

    pub fn scope<'a, F2, S>(&self, scope_fn: S)
    where
        F2: Thunk + Send + 'a,
        S: FnOnce(&mut Scope<'a, F2>),
    {
        let send_queue = unsafe { std::mem::transmute(&self.data.send_queue) };

        let mut scope = Scope::new(send_queue);
        scope_fn(&mut scope);
        let target_count = scope.consume();

        let backoff = Backoff::new();
        let mut count = 0;
        while count < target_count {
            match self.data.recv_queue.pop() {
                Ok(r) => {
                    r.unwrap();
                    count += 1;
                    backoff.reset();
                },
                Err(_) => {
                    backoff.snooze();
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
    let mut snooze_count = 0;

    while !data.stop.load(Ordering::Relaxed) {
        match data.send_queue.pop() {
            Ok(thunk) => {
                let result = catch_unwind(AssertUnwindSafe(move || thunk.call_once()));
                match data.recv_queue.push(result) {
                    Ok(()) => {
                        snooze_count = 0;
                        backoff.reset();
                    },
                    Err(_) => panic!("Error pushing to result queue"),
                }
            },
            Err(_) => {
                if snooze_count > 4096 {
                    thread::sleep(Duration::from_millis(16));
                } else {
                    snooze_count += 1;
                    backoff.snooze();
                }
            },
        }
    }
}

pub struct Scope<'a, F>
where F: Thunk + Send
{
    send_queue: &'a ArrayQueue<F>,
    counter: usize,
    inplace_tasks: Vec<F>,
}

impl<'a, F> Scope<'a, F>
where F: Thunk + Send
{
    fn new(send_queue: &'a ArrayQueue<F>) -> Scope<'a, F> {
        Scope {
            send_queue,
            counter: 0,
            inplace_tasks: Vec::new(),
        }
    }

    pub fn spawn(&mut self, thunk: F) {
        self.send_queue.push(thunk).unwrap();
        self.counter += 1;
    }

    pub fn spawn_inplace(&mut self, thunk: F) {
        self.inplace_tasks.push(thunk);
    }

    fn consume(mut self) -> usize {
        self.inplace_tasks.drain(..).for_each(|t| t.call_once());
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
    fn api_test() {
        let pool = ThreadPool::<Task>::new(2, 16, "test");

        {
            let mut number1 = 1;
            let mut number2 = 1;

            let r = pool.scope(|s| {
                s.spawn_inplace(Task {
                    0: &mut number1,
                });

                s.spawn(Task {
                    0: &mut number2,
                });
            });

            r.unwrap();

            assert_eq!(number1, 2);
            assert_eq!(number2, 2);
        }
    }
}
