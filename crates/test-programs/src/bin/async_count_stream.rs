mod bindings {
    wit_bindgen::generate!({
        path: "../misc/component-async-tests/wit",
        world: "count-stream-guest",
    });

    use super::Component;
    export!(Component);
}

use {
    bindings::{exports::local::local::count_stream::Guest, wit_stream},
    wit_bindgen::StreamReader,
};

struct Component;

impl Guest for Component {
    #[allow(async_fn_in_trait)]
    async fn get(count: u32) -> StreamReader<u32> {
        let (mut tx, rx) = wit_stream::new();

        wit_bindgen::spawn(async move {
            for i in 0..count {
                let remaining = tx.write_all(vec![i]).await;
                assert!(remaining.is_empty());
            }
        });

        rx
    }
}

// Unused function; required since this file is built as a `bin`:
fn main() {}
