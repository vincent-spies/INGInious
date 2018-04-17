# coding=utf-8
import asyncio

class InitPrefetch:
    """ Iterator that prefetch the first value of another iterator. init() must be called to start the prefetching. """

class AsyncTimeoutItr:
    """ Iterator that returns None after 5 seconds at each iteration, if the other iterator has not yet provided
        a result.
    """
    def __init__(self, itr, timeout=5):
        self.itr = itr
        self.timeout = timeout
        self.future = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.future is None:
            self.future = asyncio.ensure_future(self.itr.__anext__())

        try:
            out = await asyncio.wait_for(asyncio.shield(self.future), timeout=self.timeout)
            self.future = None
            return out
        except asyncio.TimeoutError:
            return None

class StreamWatcherWithInit:
    def __init__(self, collection, pipeline=None, full_document='default', resume_after=None,
              max_await_time_ms=None, batch_size=None, collation=None, session=None):
        self._collection = collection
        self._kwargs = {'pipeline': pipeline,
                        'full_document': full_document,
                        'resume_after': resume_after,
                        'max_await_time_ms': max_await_time_ms,
                        'batch_size': batch_size,
                        'collation': collation,
                        'session': session}
        self._delegate = None

    async def init(self):
        self._delegate = await asyncio.get_event_loop().run_in_executor(None, lambda: self._collection.delegate.watch(**self._kwargs))

    def _next(self):
        # This method is run on a thread.
        try:
            return self._delegate.next()
        except StopIteration:
            raise StopAsyncIteration()

    async def next(self):
        return await asyncio.get_event_loop().run_in_executor(None, self._next)

    async def close(self):
        self._delegate.close()

    async def __aiter__(self):
        return self

    __anext__ = next

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()