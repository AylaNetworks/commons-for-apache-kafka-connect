/*
 * Copyright 2021 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.common.handler.error;

import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaErrorProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaErrorProducer.class);
    private final String deadLetterTopic;

    private final KafkaProducer<String, String> producer;

    public KafkaErrorProducer(final String deadLetterTopic) {
        this.deadLetterTopic = deadLetterTopic == null
                ? System.getenv("AYLA_MT_KAFKA_CONNECT_PRODUCER_DLQ") : deadLetterTopic;
        final KafkaErrorProducerConfig kafkaErrorProducerConfig = new KafkaErrorProducerConfig();
        producer = new KafkaProducer<>(kafkaErrorProducerConfig.getProperties());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutdown hook triggered. Closing Kafka producer...");
            if (producer != null) {
                producer.close();
                LOGGER.info("Kafka producer closed.");
            }
        }));
    }

    public void send(String key, final String value) {
        if (key == null) {
            key = UUID.randomUUID().toString();
        }
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(deadLetterTopic, key, value);
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                LOGGER.error("Asynchronous send to deadLetterTopic error: {}, for the record: {}",
                        exception, value);
            } else {
                LOGGER.info("Asynchronous send: Sent record to topic {} partition {} with offset {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
            LOGGER.info("Kafka producer closed.");
        }
    }
}
