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

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaErrorProducerConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaErrorProducer.class);
    private final Properties properties;

    public KafkaErrorProducerConfig() {

        LOGGER.debug("Inside KafkaErrorProducerConfig constructor.");

        properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv("CONNECT_BOOTSTRAP_SERVERS") != null
                        ? System.getenv("CONNECT_BOOTSTRAP_SERVERS") : "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        updateFromEnv(properties, "CONNECT_PRODUCER_SECURITY_PROTOCOL");
        updateFromEnv(properties, "CONNECT_PRODUCER_SSL_KEYSTORE_TYPE");
        updateFromEnv(properties, "CONNECT_PRODUCER_SSL_KEYSTORE_LOCATION");
        updateFromEnv(properties, "CONNECT_PRODUCER_SSL_KEYSTORE_PASSWORD");
        updateFromEnv(properties, "CONNECT_PRODUCER_SSL_KEY_PASSWORD");
        updateFromEnv(properties, "CONNECT_PRODUCER_SSL_TRUSTSTORE_LOCATION");
        updateFromEnv(properties, "CONNECT_PRODUCER_SSL_TRUSTSTORE_PASSWORD");

        properties.put("compression.type", "snappy");
        properties.put(ProducerConfig.ACKS_CONFIG,
                System.getenv("CONNECT_PRODUCER_ACKS_CONFIG") != null
                        ? System.getenv("CONNECT_PRODUCER_ACKS_CONFIG") : "all");
        properties.put(ProducerConfig.RETRIES_CONFIG,
                System.getenv("CONNECT_PRODUCER_RETRIES_CONFIG") != null
                        ? Integer.parseInt(System.getenv("CONNECT_PRODUCER_RETRIES_CONFIG")) : 3);
        properties.put(ProducerConfig.LINGER_MS_CONFIG,
                System.getenv("CONNECT_PRODUCER_LINGER_MS_CONFIG") != null
                ? Long.parseLong(System.getenv("CONNECT_PRODUCER_LINGER_MS_CONFIG")) : 20);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,
                System.getenv("CONNECT_PRODUCER_BATCH_SIZE_CONFIG") != null
                        ? Integer.parseInt(System.getenv("CONNECT_PRODUCER_BATCH_SIZE_CONFIG")) : 32768);
    }

    private void updateFromEnv(final Properties properties, final String envVariableName) {
        if (System.getenv(envVariableName) != null) {
            switch (envVariableName) {
                case "CONNECT_PRODUCER_SECURITY_PROTOCOL":
                    properties.put("security.protocol", System.getenv("CONNECT_PRODUCER_SECURITY_PROTOCOL"));
                    break;
                case "CONNECT_PRODUCER_SSL_KEYSTORE_TYPE":
                    properties.put("ssl.keystore.type", System.getenv("CONNECT_PRODUCER_SSL_KEYSTORE_TYPE"));
                    break;
                case "CONNECT_PRODUCER_SSL_KEYSTORE_LOCATION":
                    properties.put("ssl.keystore.location",
                            System.getenv("CONNECT_PRODUCER_SSL_KEYSTORE_LOCATION"));
                    break;
                case "CONNECT_PRODUCER_SSL_KEYSTORE_PASSWORD":
                    properties.put("ssl.keystore.password",
                            System.getenv("CONNECT_PRODUCER_SSL_KEYSTORE_PASSWORD"));
                    break;
                case "CONNECT_PRODUCER_SSL_KEY_PASSWORD":
                    properties.put("ssl.key.password", System.getenv("CONNECT_PRODUCER_SSL_KEY_PASSWORD"));
                    break;
                case "CONNECT_PRODUCER_SSL_TRUSTSTORE_LOCATION":
                    properties.put("ssl.truststore.location",
                            System.getenv("CONNECT_PRODUCER_SSL_TRUSTSTORE_LOCATION"));
                    break;
                case "CONNECT_PRODUCER_SSL_TRUSTSTORE_PASSWORD":
                    properties.put("ssl.truststore.password",
                            System.getenv("CONNECT_PRODUCER_SSL_TRUSTSTORE_PASSWORD"));
                    break;
                default:
            }
        }
    }

    public Properties getProperties() {
        return properties;
    }
}
