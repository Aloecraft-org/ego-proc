TARGET_WASI:=--target wasm32-wasip2
TARGET_BROWSER:=--target wasm32-unknown-unknown
QUIET_WARN:=RUSTFLAGS="-Awarnings"

ifneq ($(filter quiet,$(MAKECMDGOALS)),)
CARGO_ENV:=$(QUIET_WARN)
else
CARGO_ENV:=
endif

quiet:
	@true

clean:
	cargo clean
	rm -rf .data

_init:
	@mkdir -p .data/home .data/tmp

define cargo_targets  # $(1)=command, $(2)=extra flags
$(1)_native:
	$(CARGO_ENV) cargo $(1)
$(1)_wasi:
	$(CARGO_ENV) cargo $(1) $(TARGET_WASI)
$(1)_browser:
	$(CARGO_ENV) cargo $(1) $(TARGET_BROWSER)
$(1): $(1)_native $(1)_wasi $(1)_browser
endef

$(eval $(call cargo_targets,build))
$(eval $(call cargo_targets,check))
$(eval $(call cargo_targets,test))

fmt:
	cargo fmt

fmt_check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --all-targets --all-features -- -D warnings
	cargo clippy --all-targets $(TARGET_WASI) -- -D warnings
	cargo clippy --all-targets $(TARGET_BROWSER) -- -D warnings

doc:
	cargo doc --no-deps --open

all: check test build

ci: fmt_check clippy check test
