use core::task;
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use component_async_tests::{Ctx, util::yield_times};
use futures::FutureExt as _;
use wasmtime::{
    Engine, Result, Store, StoreContextMut,
    component::{Linker, Source, StreamAnyConsumer, StreamResult, Val},
};
use wasmtime_wasi::{ResourceTable, WasiCtxBuilder};

use crate::scenario::util::{config, make_component};

use super::util::test_run_with_count;

// No-op function; we only test this by composing it in `async_unit_stream_caller`
#[allow(
    dead_code,
    reason = "here only to make the `assert_test_exists` macro happy"
)]
pub fn async_unit_stream_callee() {}

#[tokio::test]
pub async fn async_unit_stream_caller() -> Result<()> {
    test_run_with_count(
        &[
            test_programs_artifacts::ASYNC_UNIT_STREAM_CALLER_COMPONENT,
            test_programs_artifacts::ASYNC_UNIT_STREAM_CALLEE_COMPONENT,
        ],
        1,
    )
    .await
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
pub async fn async_unit_stream_callee_val() -> Result<()> {
    let config = config();

    let engine = Engine::new(&config)?;

    let component = make_component(
        &engine,
        &[test_programs_artifacts::ASYNC_UNIT_STREAM_CALLEE_COMPONENT],
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
    let run_instance_idx = instance
        .get_export_index(&mut store, None, "local:local/unit-stream")
        .unwrap();
    let run_idx = instance
        .get_export_index(&mut store, Some(&run_instance_idx), "run")
        .unwrap();
    let run = instance.get_func(&mut store, run_idx).unwrap();

    // Start `count` concurrent calls and then join them all:
    store
        .run_concurrent(async |store| {
            let mut results = vec![Val::Bool(false)];
            run.call_concurrent(store, &vec![Val::U32(4)], &mut results)
                .await?;
            let stream = match results.into_iter().next().unwrap() {
                Val::Stream(stream) => stream,
                _ => panic!("expected stream"),
            };

            let numbers = Arc::new(Mutex::new(Vec::<Val>::with_capacity(4)));
            // Read just one item at a time from the guest, forcing it to
            // re-take ownership of any unwritten items.
            store.with(|store| stream.pipe(store, OneAtATimeAny::new(numbers.clone(), false)))?;

            for i in 0.. {
                assert!(i < 1000);
                if 4 == numbers.lock().unwrap().len() {
                    break;
                }
                tokio::task::yield_now().await;
            }

            wasmtime::error::Ok(())
        })
        .await??;

    Ok(())
}
