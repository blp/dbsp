#![warn(unused_variables)]
#![warn(unused_mut)]
#![warn(unreachable_code)]
#![warn(dead_code)]

mod input;
mod output;
#[cfg(test)]
pub mod test;

pub use input::KafkaInputEndpoint;
pub use output::KafkaOutputEndpoint;
