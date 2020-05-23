# scoped-threadpool: Scoped threadpool for microtasks

Scoped threadpool is intended for microtasks that are CPU bound.
Implements spinning when waiting for thread results.
Propagates errors back to the main thread.
Does not allocate anything when using the scope - uses a fixed-size buffer.
The pool will panic if more tasks are pushed than slots available.
