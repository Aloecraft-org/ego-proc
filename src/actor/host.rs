pub trait HostController {
    fn new() -> Self;
    fn start_lifecycle(&mut self) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
}
