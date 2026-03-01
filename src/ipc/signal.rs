#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub trait PlatformSendSync: Send + Sync {}
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl<T: ?Sized + Send + Sync> PlatformSendSync for T {}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub trait PlatformSendSync {}
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl<T: ?Sized> PlatformSendSync for T {}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub trait ProcData: Clone + Send + Sync {}
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub trait ProcOutput: Clone + Send + Sync {}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub trait ProcData: Clone {}
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub trait ProcOutput: Clone {}

/// Unit type for actors that produce no upward output.
#[derive(Clone)]
pub struct NoOutput;
impl ProcOutput for NoOutput {}
