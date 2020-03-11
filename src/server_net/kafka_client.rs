use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::util::get_rdkafka_version;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::stream::StreamExt;
use tokio::sync::mpsc::{ Sender };
use redis::AsyncCommands;
use futures::*;
use chrono::prelude::*;


struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        //println!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        //println!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        //println!("Committing offsets: {:?}", result);
    }
}

type LoggingConsumer = StreamConsumer<CustomContext>;

pub async fn consume(brokers: &str, group_id: &str, topics: &[&str], mut sender: Sender<Vec<u8>>) {
    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        //.set("statistics.interval.ms", "30000")
        //.set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer.subscribe(&topics.to_vec()).expect("Can't subscribe to specified topics");

    // consumer.start() returns a stream. The stream can be used ot chain together expensive steps,
    // such as complex computations on a thread pool or asynchronous IO.
    println!("try start");
    let mut message_stream = consumer.start();

    while let Some(message) = message_stream.next().await {
        match message {
            Err(e) => println!("Kafka error: {}", e),
            Ok(m) => {
                let payload = m.payload().unwrap();
                let payload: Vec<u8> = payload.iter().cloned().collect();
                match sender.send(payload).await {
                    Ok(()) => {},
                    Err(_) => {
                        // TODO
                    }
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}

pub async fn produce(producer: &FutureProducer, topic_name: &str, data: &Vec<u8>) {

    let utc: DateTime<Utc> = Utc::now();
    let cur: i64 = utc.timestamp_millis();
    let future = producer
        .send(
            FutureRecord::to(topic_name)
                .payload(data)
                .key(topic_name),
            0,
        )
        .map(move |status| { 
            let utc: DateTime<Utc> = Utc::now();
            let now: i64 = utc.timestamp_millis();
        });
    future.await;
}
