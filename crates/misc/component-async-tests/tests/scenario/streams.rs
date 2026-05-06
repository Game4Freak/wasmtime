use {
    super::util::{config, make_component},
    component_async_tests::{
        Ctx, closed_streams,
        util::{
            OneshotConsumer, OneshotProducer, PipeAnyConsumer, PipeConsumer, PipeProducer,
            yield_times,
        },
    },
    futures::{
        FutureExt, Sink, SinkExt, Stream, StreamExt,
        channel::{mpsc, oneshot},
        future,
    },
    std::{
        mem,
        ops::DerefMut,
        pin::Pin,
        sync::{Arc, Mutex},
        task::{self, Context, Poll},
    },
    wasmtime::{
        Engine, Result, Store, StoreContextMut,
        component::{
            Destination, FutureReader, Lift, Linker, ResourceTable, Source, StreamAny,
            StreamAnyConsumer, StreamAnyProducer, StreamConsumer, StreamProducer, StreamReader,
            StreamResult, Val, VecBuffer,
        },
    },
    wasmtime_wasi::WasiCtxBuilder,
};

pub struct DirectPipeProducer<S>(S);

impl<D, S: Stream<Item = u8> + Send + 'static> StreamProducer<D> for DirectPipeProducer<S> {
    type Item = u8;
    type Buffer = Option<u8>;

    fn poll_produce<'a>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        store: StoreContextMut<D>,
        destination: Destination<'a, Self::Item, Self::Buffer>,
        finish: bool,
    ) -> Poll<Result<StreamResult>> {
        // SAFETY: This is a standard pin-projection, and we never move
        // out of `self`.
        let stream = unsafe { self.map_unchecked_mut(|v| &mut v.0) };

        match stream.poll_next(cx) {
            Poll::Pending => {
                if finish {
                    Poll::Ready(Ok(StreamResult::Cancelled))
                } else {
                    Poll::Pending
                }
            }
            Poll::Ready(Some(item)) => {
                let mut destination = destination.as_direct(store, 1);
                destination.remaining()[0] = item;
                destination.mark_written(1);
                Poll::Ready(Ok(StreamResult::Completed))
            }
            Poll::Ready(None) => Poll::Ready(Ok(StreamResult::Dropped)),
        }
    }
}

pub struct DirectPipeConsumer<S>(S);

impl<D, S: Sink<u8, Error: std::error::Error + Send + Sync> + Send + 'static> StreamConsumer<D>
    for DirectPipeConsumer<S>
{
    type Item = u8;

    fn poll_consume(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        store: StoreContextMut<D>,
        source: Source<Self::Item>,
        finish: bool,
    ) -> Poll<Result<StreamResult>> {
        // SAFETY: This is a standard pin-projection, and we never move
        // out of `self`.
        let mut sink = unsafe { self.map_unchecked_mut(|v| &mut v.0) };

        let on_pending = || {
            if finish {
                Poll::Ready(Ok(StreamResult::Cancelled))
            } else {
                Poll::Pending
            }
        };

        match sink.as_mut().poll_flush(cx) {
            Poll::Pending => on_pending(),
            Poll::Ready(result) => {
                result?;
                match sink.as_mut().poll_ready(cx) {
                    Poll::Pending => on_pending(),
                    Poll::Ready(result) => {
                        result?;
                        let mut source = source.as_direct(store);
                        let item = source.remaining()[0];
                        source.mark_read(1);
                        sink.start_send(item)?;
                        Poll::Ready(Ok(StreamResult::Completed))
                    }
                }
            }
        }
    }
}

#[tokio::test]
pub async fn async_closed_streams() -> Result<()> {
    let engine = Engine::new(&config())?;

    let mut store = Store::new(
        &engine,
        Ctx {
            wasi: WasiCtxBuilder::new().inherit_stdio().build(),
            table: ResourceTable::default(),
            continue_: false,
        },
    );

    let mut linker = Linker::new(&engine);

    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;

    let component = make_component(
        &engine,
        &[test_programs_artifacts::ASYNC_CLOSED_STREAMS_COMPONENT],
    )
    .await?;

    let instance = linker.instantiate_async(&mut store, &component).await?;

    let values = vec![42_u8, 43, 44];

    let value = 42_u8;

    // First, test stream host->host
    for direct_producer in [true, false] {
        for direct_consumer in [true, false] {
            let (mut input_tx, input_rx) = mpsc::channel(1);
            let (output_tx, mut output_rx) = mpsc::channel(1);
            let reader = if direct_producer {
                StreamReader::new(&mut store, DirectPipeProducer(input_rx))?
            } else {
                StreamReader::new(&mut store, PipeProducer::new(input_rx))?
            };
            if direct_consumer {
                reader.pipe(&mut store, DirectPipeConsumer(output_tx))?;
            } else {
                reader.pipe(&mut store, PipeConsumer::new(output_tx))?;
            }

            store
                .run_concurrent(async |_| {
                    let (a, b) = future::join(
                        async {
                            for &value in &values {
                                input_tx.send(value).await?;
                            }
                            drop(input_tx);
                            wasmtime::error::Ok(())
                        },
                        async {
                            for &value in &values {
                                assert_eq!(Some(value), output_rx.next().await);
                            }
                            assert!(output_rx.next().await.is_none());
                            Ok(())
                        },
                    )
                    .await;

                    a.and(b)
                })
                .await??;
        }
    }

    // Next, test futures host->host
    {
        let (input_tx, input_rx) = oneshot::channel();
        let (output_tx, output_rx) = oneshot::channel();
        FutureReader::new(&mut store, OneshotProducer::new(input_rx))?
            .pipe(&mut store, OneshotConsumer::new(output_tx))?;

        store
            .run_concurrent(async |_| {
                _ = input_tx.send(value);
                assert_eq!(value, output_rx.await?);
                wasmtime::error::Ok(())
            })
            .await??;
    }

    // Next, test stream host->guest
    {
        let (mut tx, rx) = mpsc::channel(1);
        let rx = StreamReader::new(&mut store, PipeProducer::new(rx))?;

        let closed_streams = closed_streams::bindings::ClosedStreams::new(&mut store, &instance)?;

        let values = values.clone();

        store
            .run_concurrent(async move |accessor| {
                let (a, b) = future::join(
                    async {
                        for &value in &values {
                            tx.send(value).await?;
                        }
                        drop(tx);
                        Ok(())
                    },
                    closed_streams.local_local_closed().call_read_stream(
                        accessor,
                        rx,
                        values.clone(),
                    ),
                )
                .await;

                a.and(b)
            })
            .await??;
    }

    // Next, test futures host->guest
    {
        let (tx, rx) = oneshot::channel();
        let rx = FutureReader::new(&mut store, OneshotProducer::new(rx))?;
        let (_, rx_ignored) = oneshot::channel();
        let rx_ignored = FutureReader::new(&mut store, OneshotProducer::new(rx_ignored))?;

        let closed_streams = closed_streams::bindings::ClosedStreams::new(&mut store, &instance)?;

        store
            .run_concurrent(async move |accessor| {
                _ = tx.send(value);
                closed_streams
                    .local_local_closed()
                    .call_read_future(accessor, rx, value, rx_ignored)
                    .await
            })
            .await??;
    }

    Ok(())
}

mod closed_stream {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "closed-stream-guest",
        exports: { default: store | async },
    });
}

#[tokio::test]
pub async fn async_closed_stream() -> Result<()> {
    let engine = Engine::new(&config())?;

    let component = make_component(
        &engine,
        &[test_programs_artifacts::ASYNC_CLOSED_STREAM_COMPONENT],
    )
    .await?;

    let mut linker = Linker::new(&engine);

    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;

    let mut store = Store::new(
        &engine,
        Ctx {
            wasi: WasiCtxBuilder::new().inherit_stdio().build(),
            table: ResourceTable::default(),
            continue_: false,
        },
    );

    let instance = linker.instantiate_async(&mut store, &component).await?;
    let guest = closed_stream::ClosedStreamGuest::new(&mut store, &instance)?;
    store
        .run_concurrent(async move |accessor| {
            let stream = guest.local_local_closed_stream().call_get(accessor).await?;

            let (tx, mut rx) = mpsc::channel(1);
            accessor.with(move |store| stream.pipe(store, PipeConsumer::new(tx)))?;
            assert!(rx.next().await.is_none());

            Ok(())
        })
        .await?
}

struct VecProducer<T> {
    source: Vec<T>,
    maybe_yield: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl<T> VecProducer<T> {
    fn new(source: Vec<T>, delay: bool) -> Self {
        Self {
            source,
            maybe_yield: if delay {
                yield_times(5).boxed()
            } else {
                async {}.boxed()
            },
        }
    }
}

impl<D, T: Lift + Unpin + 'static> StreamProducer<D> for VecProducer<T> {
    type Item = T;
    type Buffer = VecBuffer<T>;

    fn poll_produce(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _: StoreContextMut<D>,
        mut destination: Destination<Self::Item, Self::Buffer>,
        _: bool,
    ) -> Poll<Result<StreamResult>> {
        let maybe_yield = &mut self.as_mut().get_mut().maybe_yield;
        task::ready!(maybe_yield.as_mut().poll(cx));
        *maybe_yield = async {}.boxed();

        destination.set_buffer(mem::take(&mut self.get_mut().source).into());
        Poll::Ready(Ok(StreamResult::Dropped))
    }
}

struct OneAtATime<T> {
    destination: Arc<Mutex<Vec<T>>>,
    maybe_yield: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl<T> OneAtATime<T> {
    fn new(destination: Arc<Mutex<Vec<T>>>, delay: bool) -> Self {
        Self {
            destination,
            maybe_yield: if delay {
                yield_times(5).boxed()
            } else {
                async {}.boxed()
            },
        }
    }
}

impl<D, T: Lift + 'static> StreamConsumer<D> for OneAtATime<T> {
    type Item = T;

    fn poll_consume(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        store: StoreContextMut<D>,
        mut source: Source<Self::Item>,
        _: bool,
    ) -> Poll<Result<StreamResult>> {
        let maybe_yield = &mut self.as_mut().get_mut().maybe_yield;
        task::ready!(maybe_yield.as_mut().poll(cx));
        *maybe_yield = async {}.boxed();

        let value = &mut None;
        source.read(store, value)?;
        self.destination.lock().unwrap().push(value.take().unwrap());
        Poll::Ready(Ok(StreamResult::Completed))
    }
}

mod short_reads {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "short-reads-guest",
        exports: { default: async },
    });
}

#[tokio::test]
pub async fn async_short_reads() -> Result<()> {
    test_async_short_reads(false).await
}

#[tokio::test]
async fn async_short_reads_with_delay() -> Result<()> {
    test_async_short_reads(true).await
}

async fn test_async_short_reads(delay: bool) -> Result<()> {
    use short_reads::exports::local::local::short_reads::Thing;

    let engine = Engine::new(&config())?;

    let component = make_component(
        &engine,
        &[test_programs_artifacts::ASYNC_SHORT_READS_COMPONENT],
    )
    .await?;

    let mut linker = Linker::new(&engine);

    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;

    let mut store = Store::new(
        &engine,
        Ctx {
            wasi: WasiCtxBuilder::new().inherit_stdio().build(),
            table: ResourceTable::default(),
            continue_: false,
        },
    );

    let guest =
        short_reads::ShortReadsGuest::instantiate_async(&mut store, &component, &linker).await?;
    let thing = guest.local_local_short_reads().thing();

    let strings = ["a", "b", "c", "d", "e"];
    let mut things = Vec::with_capacity(strings.len());
    for string in strings {
        things.push(thing.call_constructor(&mut store, string).await?);
    }

    store
        .run_concurrent(async |store| {
            let count = things.len();
            let stream =
                store.with(|store| StreamReader::new(store, VecProducer::new(things, delay)))?;

            let stream = guest
                .local_local_short_reads()
                .call_short_reads(store, stream)
                .await?;

            let received_things = Arc::new(Mutex::new(Vec::<Thing>::with_capacity(count)));
            // Read just one item at a time from the guest, forcing it to
            // re-take ownership of any unwritten items.
            store.with(|store| {
                stream.pipe(store, OneAtATime::new(received_things.clone(), delay))
            })?;

            for i in 0.. {
                assert!(i < 1000);
                if count == received_things.lock().unwrap().len() {
                    break;
                }
                tokio::task::yield_now().await;
            }

            let mut received_strings = Vec::with_capacity(strings.len());
            let received_things = mem::take(received_things.lock().unwrap().deref_mut());
            for it in received_things {
                received_strings.push(thing.call_get(store, it).await?);
            }

            assert_eq!(
                &strings[..],
                &received_strings
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
            );

            wasmtime::error::Ok(())
        })
        .await?
}

// ------------ VAL ----------------

#[tokio::test]
pub async fn async_closed_stream_val() -> Result<()> {
    let engine = Engine::new(&config())?;

    let component = make_component(
        &engine,
        &[test_programs_artifacts::ASYNC_CLOSED_STREAM_COMPONENT],
    )
    .await?;

    let mut linker = Linker::new(&engine);

    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;

    let mut store = Store::new(
        &engine,
        Ctx {
            wasi: WasiCtxBuilder::new().inherit_stdio().build(),
            table: ResourceTable::default(),
            continue_: false,
        },
    );

    let instance = linker.instantiate_async(&mut store, &component).await?;
    let closed_stream_instance_idx = instance
        .get_export_index(&mut store, None, "local:local/closed-stream")
        .unwrap();
    let closed_stream_get_idx = instance
        .get_export_index(&mut store, Some(&closed_stream_instance_idx), "get")
        .unwrap();
    let closed_stream_get = instance
        .get_func(&mut store, closed_stream_get_idx)
        .unwrap();

    store
        .run_concurrent(async move |accessor| {
            let mut results = vec![Val::Bool(false)];
            closed_stream_get
                .call_concurrent(accessor, &vec![], &mut results)
                .await?;
            let stream = match results.into_iter().next().unwrap() {
                Val::Stream(stream) => stream,
                _ => panic!("expected stream"),
            };

            let (tx, mut rx) = mpsc::channel(1);
            accessor.with(move |store| stream.pipe(store, PipeAnyConsumer::new(tx)))?;
            assert!(rx.next().await.is_none());

            Ok(())
        })
        .await?
}

struct VecAnyProducer {
    source: Vec<Val>,
    maybe_yield: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl VecAnyProducer {
    fn new(source: Vec<Val>, delay: bool) -> Self {
        Self {
            source,
            maybe_yield: if delay {
                yield_times(5).boxed()
            } else {
                async {}.boxed()
            },
        }
    }
}

impl<D> StreamAnyProducer<D> for VecAnyProducer {
    type Buffer = VecBuffer<Val>;

    fn poll_produce(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _: StoreContextMut<D>,
        mut destination: Destination<Val, Self::Buffer>,
        _: bool,
    ) -> Poll<Result<StreamResult>> {
        let maybe_yield = &mut self.as_mut().get_mut().maybe_yield;
        task::ready!(maybe_yield.as_mut().poll(cx));
        *maybe_yield = async {}.boxed();

        destination.set_buffer(mem::take(&mut self.get_mut().source).into());
        Poll::Ready(Ok(StreamResult::Dropped))
    }
}

struct OneAtATimeAny {
    destination: Arc<Mutex<Vec<Val>>>,
    maybe_yield: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl OneAtATimeAny {
    fn new(destination: Arc<Mutex<Vec<Val>>>, delay: bool) -> Self {
        Self {
            destination,
            maybe_yield: if delay {
                yield_times(5).boxed()
            } else {
                async {}.boxed()
            },
        }
    }
}

impl<D> StreamAnyConsumer<D> for OneAtATimeAny {
    fn poll_consume(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        store: StoreContextMut<D>,
        mut source: Source<Val>,
        _: bool,
    ) -> Poll<Result<StreamResult>> {
        let maybe_yield = &mut self.as_mut().get_mut().maybe_yield;
        task::ready!(maybe_yield.as_mut().poll(cx));
        *maybe_yield = async {}.boxed();

        let value = &mut None;
        source.read_val(store, value)?;
        self.destination.lock().unwrap().push(value.take().unwrap());
        Poll::Ready(Ok(StreamResult::Completed))
    }
}

#[tokio::test]
pub async fn async_short_reads_val() -> Result<()> {
    test_async_short_reads_val(false).await
}

#[tokio::test]
async fn async_short_reads_val_with_delay() -> Result<()> {
    test_async_short_reads_val(true).await
}

async fn test_async_short_reads_val(delay: bool) -> Result<()> {
    let engine = Engine::new(&config())?;

    let component = make_component(
        &engine,
        &[test_programs_artifacts::ASYNC_SHORT_READS_COMPONENT],
    )
    .await?;

    let mut linker = Linker::new(&engine);

    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;

    let mut store = Store::new(
        &engine,
        Ctx {
            wasi: WasiCtxBuilder::new().inherit_stdio().build(),
            table: ResourceTable::default(),
            continue_: false,
        },
    );

    let guest = linker.instantiate_async(&mut store, &component).await?;
    let short_reads_instance_idx = guest
        .get_export_index(&mut store, None, "local:local/short-reads")
        .unwrap();
    let thing_constructor_idx = guest
        .get_export_index(
            &mut store,
            Some(&short_reads_instance_idx),
            "[constructor]thing",
        )
        .unwrap();
    let thing_constructor = guest.get_func(&mut store, thing_constructor_idx).unwrap();
    let thing_get_idx = guest
        .get_export_index(
            &mut store,
            Some(&short_reads_instance_idx),
            "[method]thing.get",
        )
        .unwrap();
    let thing_get = guest.get_func(&mut store, thing_get_idx).unwrap();

    let strings = ["a", "b", "c", "d", "e"];
    let mut things = Vec::with_capacity(strings.len());
    for string in strings {
        let mut results = vec![Val::Bool(false)];
        thing_constructor
            .call_async(&mut store, &[Val::String(string.to_string())], &mut results)
            .await?;
        things.push(results.into_iter().next().unwrap());
    }

    let short_reads_idx = guest
        .get_export_index(&mut store, Some(&short_reads_instance_idx), "short-reads")
        .unwrap();
    let short_reads = guest.get_func(&mut store, short_reads_idx).unwrap();

    store
        .run_concurrent(async |store| {
            let count = things.len();
            let stream =
                store.with(|store| StreamAny::new(store, VecAnyProducer::new(things, delay)))?;

            let mut results = vec![Val::Bool(false)];
            short_reads
                .call_concurrent(store, &[Val::Stream(stream)], &mut results)
                .await?;
            let stream = match results.into_iter().next().unwrap() {
                Val::Stream(stream) => stream,
                _ => panic!("expected stream"),
            };

            let received_things = Arc::new(Mutex::new(Vec::<Val>::with_capacity(count)));
            // Read just one item at a time from the guest, forcing it to
            // re-take ownership of any unwritten items.
            store.with(|store| {
                stream.pipe(store, OneAtATimeAny::new(received_things.clone(), delay))
            })?;

            for i in 0.. {
                assert!(i < 1000);
                if count == received_things.lock().unwrap().len() {
                    break;
                }
                tokio::task::yield_now().await;
            }

            let mut received_strings = Vec::with_capacity(strings.len());
            let received_things = mem::take(received_things.lock().unwrap().deref_mut());
            for it in received_things {
                let mut results = vec![Val::Bool(false)];
                thing_get
                    .call_concurrent(store, &[it], &mut results)
                    .await?;
                received_strings.push(results.into_iter().next().unwrap());
            }

            assert_eq!(
                &strings[..],
                &received_strings
                    .iter()
                    .map(|s| match s {
                        Val::String(s) => s.as_str(),
                        _ => panic!("expected string"),
                    })
                    .collect::<Vec<_>>()
            );

            wasmtime::error::Ok(())
        })
        .await?
}

mod count_stream {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "count-stream-guest",
        exports: { default: async },
    });
}

#[tokio::test]
pub async fn async_count_stream() -> Result<()> {
    let config = config();

    let engine = Engine::new(&config)?;

    let component = make_component(
        &engine,
        &[test_programs_artifacts::ASYNC_COUNT_STREAM_COMPONENT],
    )
    .await?;

    let mut linker = Linker::new(&engine);

    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;

    let mut store = Store::new(
        &engine,
        Ctx {
            wasi: WasiCtxBuilder::new().inherit_stdio().build(),
            table: ResourceTable::default(),
            continue_: false,
        },
    );

    let guest =
        count_stream::CountStreamGuest::instantiate_async(&mut store, &component, &linker).await?;

    // Start `count` concurrent calls and then join them all:
    store
        .run_concurrent(async |store| {
            let count = 4;

            let stream = guest
                .local_local_count_stream()
                .call_get(store, count)
                .await?;

            let (tx, mut rx) = mpsc::channel(1);
            store.with(|store| stream.pipe(store, PipeConsumer::new(tx)))?;

            for c in 1..count {
                assert_eq!(rx.next().await, Some(c));
            }

            wasmtime::error::Ok(())
        })
        .await??;

    Ok(())
}

#[tokio::test]
pub async fn async_count_stream_val() -> Result<()> {
    let config = config();

    let engine = Engine::new(&config)?;

    let component = make_component(
        &engine,
        &[test_programs_artifacts::ASYNC_COUNT_STREAM_COMPONENT],
    )
    .await?;

    let mut linker = Linker::new(&engine);

    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;

    let mut store = Store::new(
        &engine,
        Ctx {
            wasi: WasiCtxBuilder::new().inherit_stdio().build(),
            table: ResourceTable::default(),
            continue_: false,
        },
    );

    let instance = linker.instantiate_async(&mut store, &component).await?;
    let count_stream_instance_idx = instance
        .get_export_index(&mut store, None, "local:local/count-stream")
        .unwrap();
    let get_idx = instance
        .get_export_index(&mut store, Some(&count_stream_instance_idx), "get")
        .unwrap();
    let get = instance.get_func(&mut store, get_idx).unwrap();

    // Start `count` concurrent calls and then join them all:
    store
        .run_concurrent(async |store| {
            let count = 4;

            let mut results = vec![Val::Bool(false)];
            get.call_concurrent(store, &vec![Val::U32(count)], &mut results)
                .await?;
            let stream = match results.into_iter().next().unwrap() {
                Val::Stream(stream) => stream,
                _ => panic!("expected stream"),
            };

            let (tx, mut rx) = mpsc::channel(1);
            store.with(|store| stream.pipe(store, PipeAnyConsumer::new(tx)))?;

            for c in 1..count {
                assert_eq!(rx.next().await, Some(Val::U32(c)));
            }

            wasmtime::error::Ok(())
        })
        .await??;

    Ok(())
}
