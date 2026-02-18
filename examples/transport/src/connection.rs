use aloeproc::actor::ActorState;
use aloeproc::primitives::{ControlSignal, ProcData, ProcOutput};
use async_trait::async_trait;

// Define what the worker sends back
#[derive(Debug, Clone)]
pub enum ConnectionOutput {
    BytesReceived(usize),
    ConnectionClosed(u32), // ID
}
impl ProcOutput for ConnectionOutput {}

#[derive(Debug, Clone)]
pub struct ConnectionData(pub Vec<u8>); // Represents a packet
impl ProcData for ConnectionData {}

pub struct ConnectionState {
    pub id: u32,
    output_buffer: Vec<ConnectionOutput>,
}

impl ConnectionState {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            output_buffer: Vec::new(),
        }
    }
}

#[async_trait]
impl ActorState for ConnectionState {
    type D = ConnectionData;
    type O = ConnectionOutput;

    async fn on_tick(&mut self) -> anyhow::Result<bool> {
        self.output_buffer.push(ConnectionOutput::BytesReceived(1024));
        Ok(true)
    }

    async fn on_signal(&mut self, _sig: ControlSignal) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_data(&mut self, data: Self::D) -> anyhow::Result<()> {
        println!("[Conn {}] Sending packet: {:?}", self.id, data);
        Ok(())
    }

    fn take_output(&mut self) -> Vec<Self::O> {
        std::mem::take(&mut self.output_buffer)
    }
}
