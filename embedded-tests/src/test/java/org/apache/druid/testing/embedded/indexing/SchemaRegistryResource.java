package org.apache.druid.testing.embedded.indexing;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;


/**
 * A resource for managing a Schema Registry instance in embedded tests.
 * This class extends TestcontainerResource to provide a Schema Registry container.
 * Confluent schema registry is commonly used for managing schemas in Kafka-based applications but not
 * natively supported by testcontainers.
 */
public class SchemaRegistryResource extends TestcontainerResource<GenericContainer<?>>
{
  private static final String SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:latest";

  KafkaContainer kafkaContainer;

  @Override
  protected GenericContainer<?> createContainer()
  {
    try {
    Thread.sleep(6000000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for Kafka to start", e);
    }
    return new GenericContainer<>(SCHEMA_REGISTRY_IMAGE)
        .withNetwork(Network.SHARED)
        .dependsOn(kafkaContainer)
        .withExposedPorts(9081)
        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:9081")
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", StringUtils.format("%s:9092", "kafkatc"));
  }
}
