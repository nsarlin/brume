use bytes::Bytes;
use futures::stream::{Stream, TryStreamExt};
use futures::TryStream;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

/// A stream that counts the number of bytes that goes through it
pub(super) struct ByteCounter<S> {
    inner: S,
    counter: Arc<AtomicU64>,
}

impl<S, E> Stream for ByteCounter<S>
where
    S: TryStream<Ok = Bytes, Error = E> + Unpin,
{
    type Item = Result<Bytes, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.try_poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                self.counter.fetch_add(bytes.len() as u64, Ordering::SeqCst);
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub(super) trait ByteCounterExt: TryStream<Ok = Bytes> {
    fn count_bytes(self, counter: Arc<AtomicU64>) -> ByteCounter<Self>
    where
        Self: Sized,
    {
        ByteCounter {
            inner: self,
            counter,
        }
    }
}

impl<T: TryStream<Ok = Bytes>> ByteCounterExt for T {}
