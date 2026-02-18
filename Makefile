check_wasi:
	cargo check --target=wasm32-wasip2
check_browser:
	cargo check --target=wasm32-unknown-unknown
check_native:
	cargo check
check: check_native check_wasi check_browser

test_wasi:
	cargo test --target=wasm32-wasip2
test_browser:
	cargo test --target=wasm32-unknown-unknown
test_native:
	cargo test
test: test_native test_wasi test_browser

# RUSTFLAGS="-Awarnings" 