//! Shared test attribute aliases so the same test source runs everywhere:
//! `#[test]` for sync tests and `#[async_test]` for async tests.
//!
//! - **Native/WASI**: `test` is the built-in harness attribute and
//!   `async_test` is `tokio::test`.
//! - **Browser**: both map to `wasm_bindgen_test`, which runs the test
//!   inside a headless browser via `wasm-bindgen-test-runner`.

#![allow(unused_imports)]

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use wasm_bindgen_test::wasm_bindgen_test as async_test;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use wasm_bindgen_test::wasm_bindgen_test as test;

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use core::prelude::v1::test;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use tokio::test as async_test;
