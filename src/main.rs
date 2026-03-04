mod cli;
mod descriptors;
mod schema_registry;
mod util;

use std::borrow::Cow;
use std::env;
use std::ffi::CString;
use std::io::{Cursor, Seek, SeekFrom};
use std::time::Duration;

use byteorder::{BigEndian, ReadBytesExt};
use clap::Parser;
use eyre::{bail, eyre};
use integer_encoding::VarInt;
use prost::Message as _;
use prost_reflect::DynamicMessage;
use rdkafka::bindings::{
    RD_KAFKA_EVENT_DESCRIBETOPICS_RESULT, rd_kafka_DescribeTopics,
    rd_kafka_DescribeTopics_result_topics, rd_kafka_TopicCollection_destroy,
    rd_kafka_TopicCollection_of_topic_names, rd_kafka_TopicDescription_partitions,
    rd_kafka_TopicPartitionInfo_partition, rd_kafka_event_DescribeTopics_result,
    rd_kafka_event_destroy, rd_kafka_event_type, rd_kafka_queue_destroy, rd_kafka_queue_new,
    rd_kafka_queue_poll,
};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message as _, TopicPartitionList};
use serde::{Deserialize, Serialize};
use tracing::error;
use tracing::level_filters::LevelFilter;

use crate::cli::{Args, Command};
use crate::descriptors::DescriptorPool;
use crate::schema_registry::{SchemaRegistryClient, SchemaVersionOrLatest};
use crate::util::decode_varint;

#[derive(Serialize, Deserialize)]
struct Record {
    key: Option<String>,
    value: serde_json::Value,
}

fn main() -> eyre::Result<()> {
    color_eyre::install()?;

    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();

    let Some(brokers) = args.brokers.or_else(|| env::var("KAFE_KAFKA_BROKERS").ok()) else {
        bail!("No kafka brokers specified")
    };

    let Some(schema_registry) = args
        .schema_registry
        .or_else(|| env::var("KAFE_SCHEMA_REGISTRY").ok())
    else {
        bail!("No schema registry specified")
    };

    let schema_registry = SchemaRegistryClient::new(format!("http://{schema_registry}"));
    let mut descriptor_pool = DescriptorPool::new(&schema_registry);

    match args.command {
        Command::Produce {
            topic,
            message: message_name,
        } => {
            // TODO: Support other subject strategies
            let subject = format!("{topic}-value");

            let message_descriptor = descriptor_pool
                .get_message_descriptor_by_name(&subject, message_name.as_deref())?;

            let producer: ThreadedProducer<DefaultProducerContext> = ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .create()?;

            let schema = schema_registry
                .get_subject_schema_version(subject, SchemaVersionOrLatest::Latest)?;

            for line in std::io::stdin().lines() {
                let Ok(line) = line else { continue };
                let Ok(record) = serde_json::from_str::<Record>(&line) else {
                    error!("invalid record: {line}");
                    continue;
                };

                let message =
                    DynamicMessage::deserialize(message_descriptor.clone(), record.value)?;

                let mut bytes = Vec::with_capacity(0);

                bytes.push(0u8);
                bytes.extend_from_slice(&schema.id.to_be_bytes());
                bytes.extend_from_slice(&1.encode_var_vec());
                bytes.extend_from_slice(&0.encode_var_vec());
                bytes.extend_from_slice(&message.encode_to_vec());

                let mut kafka_record = BaseRecord::to(&topic).payload(&bytes);

                if let Some(key) = record.key.as_deref() {
                    kafka_record = kafka_record.key(key);
                }

                producer.send(kafka_record).unwrap();
            }

            producer.flush(Timeout::After(Duration::from_secs(5)))?;
        }
        Command::Consume { topic } => {
            let consumer: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &brokers)
                .set("group.id", "kafe")
                .set("enable.auto.commit", "false")
                .create()?;

            let partitions = consumer.partitions(&topic)?;

            let mut assignment = TopicPartitionList::new();
            for partition in partitions {
                assignment.add_partition_offset(&topic, partition, rdkafka::Offset::Beginning)?;
            }

            consumer.assign(&assignment)?;

            loop {
                let Some(res) = consumer.poll(Timeout::Never) else {
                    continue;
                };

                let msg = match res {
                    Ok(res) => res,
                    Err(e) => {
                        error!("{e}");
                        continue;
                    }
                };

                let Some(payload) = msg.payload() else {
                    continue;
                };

                let mut cursor = Cursor::new(payload);

                // Skip over magic byte
                // TODO: validate
                cursor.seek(SeekFrom::Start(1))?;

                // TODO: Use exact schema id
                let _schema_id = cursor.read_u32::<BigEndian>()?;

                if decode_varint(&mut cursor)? != 1 {
                    bail!("Nested messages are not yet supported");
                }

                let message_index = usize::try_from(decode_varint(&mut cursor)?)?;
                let message_byes = &payload[cursor.position() as usize..];

                // TODO: Support other subject strategies
                let subject = format!("{topic}-value");

                let file_descriptor = descriptor_pool.get_file_descriptor(&subject)?;
                let message_descriptor =
                    file_descriptor
                        .messages()
                        .nth(message_index)
                        .ok_or_else(|| {
                            eyre!("Invalid message index {message_index} for schema '{subject}'")
                        })?;

                let message = DynamicMessage::decode(message_descriptor, message_byes)?;
                let json = serde_json::to_value(&message)?;

                let record = Record {
                    key: msg.key().map(String::from_utf8_lossy).map(Cow::into_owned),
                    value: json,
                };

                let record_json = serde_json::to_string(&record)?;

                println!("{record_json}")
            }
        }
    }

    Ok(())
}

trait KafkaConsumerExt<C: rdkafka::ClientContext>: Consumer {
    fn partitions(&self, topic: &str) -> eyre::Result<Vec<i32>> {
        let topic = CString::new(topic)?;
        let mut topics = [topic.as_ptr()];
        let topics = unsafe { rd_kafka_TopicCollection_of_topic_names(topics.as_mut_ptr(), 1) };
        let queue = unsafe { rd_kafka_queue_new(self.client().native_ptr()) };

        unsafe {
            rd_kafka_DescribeTopics(self.client().native_ptr(), topics, std::ptr::null(), queue)
        };

        let event = loop {
            let event = unsafe { rd_kafka_queue_poll(queue, 1000) };
            if unsafe { rd_kafka_event_type(event) } == RD_KAFKA_EVENT_DESCRIBETOPICS_RESULT {
                break event;
            }
            unsafe { rd_kafka_event_destroy(event) };
        };

        let result = unsafe { rd_kafka_event_DescribeTopics_result(event) };

        let mut topic_descriptions_count = 0;
        let topic_descriptions =
            unsafe { rd_kafka_DescribeTopics_result_topics(result, &mut topic_descriptions_count) };

        assert!(topic_descriptions_count == 1);

        let mut topic_partition_infos_count = 0;
        let topic_partition_infos = unsafe {
            rd_kafka_TopicDescription_partitions(
                *topic_descriptions,
                &mut topic_partition_infos_count,
            )
        };

        let mut partitions = vec![];
        for i in 0..topic_partition_infos_count {
            partitions.push(unsafe {
                rd_kafka_TopicPartitionInfo_partition(*topic_partition_infos.add(i))
            });
        }

        unsafe {
            rd_kafka_TopicCollection_destroy(topics);
            rd_kafka_event_destroy(event);
            rd_kafka_queue_destroy(queue);
        }

        Ok(partitions)
    }
}

impl<C: rdkafka::consumer::ConsumerContext> KafkaConsumerExt<C> for BaseConsumer<C> where
    Self: Consumer
{
}
