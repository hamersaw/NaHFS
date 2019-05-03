use crossbeam_channel::{self, Receiver, Sender};

use std::thread::JoinHandle;
use std::time::Duration;

pub struct NamenodeProtocol {
    block_report_duration: Duration,
    heartbeat_duration: Duration,
    join_handle: Option<JoinHandle<()>>,
    shutdown_channel: (Sender<bool>, Receiver<bool>),
}

impl NamenodeProtocol {
    pub fn new(heartbeat_ms: u64, block_report_ms: u64)
            -> NamenodeProtocol {
        NamenodeProtocol {
            block_report_duration: Duration::from_millis(block_report_ms),
            heartbeat_duration: Duration::from_millis(heartbeat_ms),
            join_handle: None,
            shutdown_channel: crossbeam_channel::bounded(4),
        }
    }

    pub fn start(&mut self) {
        let shutdown_receiver = self.shutdown_channel.1.clone();
        let block_report_tick = 
            crossbeam_channel::tick(self.block_report_duration);
        let heartbeat_tick = 
            crossbeam_channel::tick(self.heartbeat_duration);

        let join_handle = std::thread::spawn(move || {
            loop {
                select! {
                    recv(block_report_tick) -> _ => {
                        println!("TODO - block report");
                    },
                    recv(heartbeat_tick) -> _ => {
                        println!("TODO - heartbeat");
                    },
                    recv(shutdown_receiver) -> _ => break,
                }
            }
        });

        self.join_handle = Some(join_handle);
    }

    pub fn stop(mut self) {
        if let Some(join_handle) = self.join_handle {
            self.shutdown_channel.0.send(true);
            join_handle.join();
        }

        self.join_handle = None;
    }
}
