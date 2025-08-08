/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.testing.embedded.indexing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.avro.AvroExtensionsModule;
import org.apache.druid.data.input.avro.AvroStreamInputFormat;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.protobuf.FileBasedProtobufBytesDecoder;
import org.apache.druid.data.input.protobuf.ProtobufExtensionsModule;
import org.apache.druid.data.input.protobuf.ProtobufInputFormat;
import org.apache.druid.data.input.protobuf.SchemaRegistryBasedProtobufBytesDecoder;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorTuningConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.tools.EventSerializer;
import org.apache.druid.testing.tools.StreamGenerator;
import org.apache.druid.testing.tools.WikipediaStreamEventStreamGenerator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Embedded test of kafka indexing for different data formats.
 */
public class KafkaDataFormatsTest extends EmbeddedClusterTestBase
{
  private static final long CYCLE_PADDING_MS = 100;
  private static final int EVENTS_PER_SECOND = 6;


  private static ObjectMapper jsonMapper;
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private KafkaResource kafkaServer;
  private SchemaRegistryResource schemaRegistry;

  @BeforeAll
  public static void beforeAll()
  {
    jsonMapper = TestHelper.JSON_MAPPER;
  }

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper();

    kafkaServer = new KafkaResource()
    {
      @Override
      public void start()
      {
        super.start();
      }

      @Override
      public void stop()
      {
        deleteTopic(dataSource);
        super.stop();
      }
    };

    schemaRegistry = new SchemaRegistryResource()
    {
      @Override
      public void start()
      {
        this.kafkaContainer = kafkaServer.getContainer();
        super.start();
      }

      @Override
      public void stop()
      {
        super.stop();
      }
    };

    indexer.addProperty("druid.segment.handoff.pollDuration", "PT0.1s")
           .addProperty("druid.worker.capacity", "10");
    overlord.addProperty("druid.indexer.task.default.context", "{\"useConcurrentLocks\": true}")
            .addProperty("druid.manager.segments.useIncrementalCache", "ifSynced")
            .addProperty("druid.manager.segments.pollDuration", "PT0.1s")
            .addProperty("druid.manager.segments.killUnused.enabled", "true")
            .addProperty("druid.manager.segments.killUnused.bufferPeriod", "PT0.1s")
            .addProperty("druid.manager.segments.killUnused.dutyPeriod", "PT1s");
    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "ifSynced");
    cluster.addExtension(KafkaIndexTaskModule.class)
           .addExtension(ProtobufExtensionsModule.class)
           .addExtension(AvroExtensionsModule.class)
           .useLatchableEmitter()
           .addCommonProperty("druid.monitoring.emissionPeriod", "PT0.1s")
           .addCommonProperty(
               "druid.monitoring.monitors",
               "[\"org.apache.druid.java.util.metrics.JvmMonitor\","
               + "\"org.apache.druid.server.metrics.TaskCountStatsMonitor\"]"
           )
           .addResource(kafkaServer)
           .addResource(schemaRegistry)
           .addServer(coordinator)
           .addServer(overlord)
           .addServer(indexer)
           .addServer(broker)
           .addServer(historical);

    return cluster;
  }

  @Test
  public void test_indexKafka_avroDataFormat() throws IOException
  {
    new AvroExtensionsModule().getJacksonModules().forEach(jsonMapper::registerModule);
    kafkaServer.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = jsonMapper.readValue("{\"type\": \"avro\"}", EventSerializer.class);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, false);
    AvroStreamInputFormat inputFormat = jsonMapper.readValue(
        getClass().getResourceAsStream("/test-data/avro/input_format.json"),
        AvroStreamInputFormat.class
    );
    KafkaSupervisorSpec supervisorSpec = createKafkaSupervisor(dataSource, dataSource, inputFormat);
    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    // Wait for a task to succeed
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/success/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(1)
    );
    // Wait for the broker to discover the realtime segments
    broker.latchableEmitter().waitForEvent(
        event -> event.hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );


    // Verify the count of rows ingested into the datasource so far
    Assertions.assertEquals(StringUtils.format("%d", recordCount), cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
  }

  @Test
  public void test_indexKafka_avroDataFormatWithSchemaRegistry()
  {
    kafkaServer.createTopicWithPartitions(dataSource, 3);
  }

  @Test
  public void test_indexKafka_csvDataFormat() throws JsonProcessingException
  {
    kafkaServer.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = jsonMapper.readValue("{\"type\": \"csv\"}", EventSerializer.class);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, false);
    CsvInputFormat inputFormat = new CsvInputFormat(List.of("timestamp","page","language","user","unpatrolled","newPage","robot","anonymous","namespace","continent","country","region","city","added","deleted","delta"), null, null, false, 0, false);
    KafkaSupervisorSpec supervisorSpec = createKafkaSupervisor(dataSource, dataSource, inputFormat);

    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    // Wait for a task to succeed
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/success/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(1)
    );
    // Wait for the broker to discover the realtime segments
    broker.latchableEmitter().waitForEvent(
        event -> event.hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );

    // Verify the count of rows ingested into the datasource so far
    Assertions.assertEquals(StringUtils.format("%d", recordCount), cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
  }

  @Test
  public void test_indexKafka_jsonDataFormat() throws JsonProcessingException
  {
    kafkaServer.createTopicWithPartitions(dataSource, 3);

    EventSerializer serializer = jsonMapper.readValue("{\"type\": \"json\"}", EventSerializer.class);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, false);
    InputFormat inputFormat = new JsonInputFormat(null, null, null, false, null, null);
    KafkaSupervisorSpec supervisorSpec = createKafkaSupervisor(dataSource, dataSource, inputFormat);

    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    // Wait for a task to succeed
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/success/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(1)
    );
    // Wait for the broker to discover the realtime segments
    broker.latchableEmitter().waitForEvent(
        event -> event.hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );

    // Verify the count of rows ingested into the datasource so far
    Assertions.assertEquals(StringUtils.format("%d", recordCount), cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
  }

  @Test
  public void test_indexKafka_protobufDataFormat() throws JsonProcessingException
  {
    new ProtobufExtensionsModule().getJacksonModules().forEach(jsonMapper::registerModule);

    kafkaServer.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = jsonMapper.readValue("{\"type\": \"protobuf\"}", EventSerializer.class);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, false);

    FileBasedProtobufBytesDecoder protobufBytesDecoder = jsonMapper.readValue(
        "{\"type\": \"file\", \"protoMessageType\": \"Wikipedia\", \"descriptor\": \"test-data/protobuf/wikipedia.desc\"}",
        FileBasedProtobufBytesDecoder.class
    );

    ProtobufInputFormat inputFormat = new ProtobufInputFormat(null, protobufBytesDecoder);

    KafkaSupervisorSpec supervisorSpec = createKafkaSupervisor(dataSource, dataSource, inputFormat);

    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    // Wait for a task to succeed
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/success/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(1)
    );
    // Wait for the broker to discover the realtime segments
    broker.latchableEmitter().waitForEvent(
        event -> event.hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );

    // Verify the count of rows ingested into the datasource so far
    Assertions.assertEquals(StringUtils.format("%d", recordCount), cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
  }

  @Test
  public void test_indexKafka_protobufDataFormatWithSchemaRegistry() throws JsonProcessingException
  {
    new ProtobufExtensionsModule().getJacksonModules().forEach(jsonMapper::registerModule);

    kafkaServer.createTopicWithPartitions(dataSource,  3);
    EventSerializer serializer = jsonMapper.readValue("{\"type\": \"protobuf\"}", EventSerializer.class);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, false);
    SchemaRegistryBasedProtobufBytesDecoder protobufBytesDecoder = jsonMapper.readValue(
        StringUtils.format("{\"type\": \"schema_registry\", \"urls\": [\"http://localhost:%s\"]}",
            schemaRegistry.getContainer().getExposedPorts().get(0)),
        SchemaRegistryBasedProtobufBytesDecoder.class
    );
    ProtobufInputFormat inputFormat = new ProtobufInputFormat(null, protobufBytesDecoder);
    KafkaSupervisorSpec supervisorSpec = createKafkaSupervisor(dataSource, dataSource, inputFormat);
    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );

    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    // Wait for a task to succeed
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/success/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(1)
    );
    // Wait for the broker to discover the realtime segments
    broker.latchableEmitter().waitForEvent(
        event -> event.hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );

    // Verify the count of rows ingested into the datasource so far
    Assertions.assertEquals(StringUtils.format("%d", recordCount), cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
  }

  @Test
  public void test_indexKafka_tsvDataFormat() throws JsonProcessingException
  {
    kafkaServer.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = jsonMapper.readValue("{\"type\": \"tsv\"}", EventSerializer.class);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, false);
    DelimitedInputFormat inputFormat = new DelimitedInputFormat(List.of("timestamp", "page", "language", "user", "unpatrolled", "newPage", "robot", "anonymous", "namespace", "continent", "country", "region", "city", "added", "deleted", "delta"), null, null, false, false, 0, null);
    KafkaSupervisorSpec supervisorSpec = createKafkaSupervisor(dataSource, dataSource, inputFormat);

    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    // Wait for a task to succeed
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/success/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(1)
    );
    // Wait for the broker to discover the realtime segments
    broker.latchableEmitter().waitForEvent(
        event -> event.hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );

    // Verify the count of rows ingested into the datasource so far
    Assertions.assertEquals(StringUtils.format("%d", recordCount), cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
  }

  private int generateStreamAndPublishToKafka(String topic, EventSerializer serializer, boolean useSchemaRegistry)
  {
    final StreamGenerator streamGenerator = new WikipediaStreamEventStreamGenerator(
        serializer,
        EVENTS_PER_SECOND,
        CYCLE_PADDING_MS
    );
    List<byte[]> records = streamGenerator.generate(10);

    ArrayList<ProducerRecord<byte[], byte[]>> producerRecords = new ArrayList<>();
    for (byte[] record : records) {
      producerRecords.add(new ProducerRecord<>(topic, record));
    }

    if (useSchemaRegistry) {
      kafkaServer.produceRecordsToTopicWithExtraProperties(
          producerRecords,
          Map.of("schema.registry.url", "http://localhost:" + schemaRegistry.getContainer().getExposedPorts().get(0))
      );
    } else {
      kafkaServer.produceRecordsToTopic(producerRecords);
    }
    return producerRecords.size();
  }

  private KafkaSupervisorSpec createKafkaSupervisor(String supervisorId, String topic, InputFormat inputFormat)
  {
    return new KafkaSupervisorSpec(
        supervisorId,
        null,
        DataSchema.builder()
                  .withDataSource(dataSource)
                  .withTimestamp(new TimestampSpec("timestamp", null, null))
                  .withDimensions(DimensionsSpec.EMPTY)
                  .build(),
        createTuningConfig(),
        new KafkaSupervisorIOConfig(
            topic,
            null,
            inputFormat,
            null, null,
            Period.seconds(1),
            kafkaServer.consumerProperties(),
            null, null, null, null, null,
            true,
            null, null, null, null, null, null, null, null
        ),
        null, null, null, null, null, null, null, null, null, null, null
    );
  }

  private KafkaSupervisorTuningConfig createTuningConfig()
  {
    return new KafkaSupervisorTuningConfig(
        null,
        null, null, null,
        1,
        null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null
    );
  }

}
