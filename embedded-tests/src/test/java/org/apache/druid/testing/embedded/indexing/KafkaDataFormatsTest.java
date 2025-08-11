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
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.avro.AvroExtensionsModule;
import org.apache.druid.data.input.avro.AvroStreamInputFormat;
import org.apache.druid.data.input.avro.InlineSchemaAvroBytesDecoder;
import org.apache.druid.data.input.avro.SchemaRegistryBasedAvroBytesDecoder;
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
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.tools.AvroSchemaRegistryEventSerializer;
import org.apache.druid.testing.tools.EventSerializer;
import org.apache.druid.testing.tools.ProtobufSchemaRegistryEventSerializer;
import org.apache.druid.testing.tools.StreamGenerator;
import org.apache.druid.testing.tools.WikipediaStreamEventStreamGenerator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Embedded test of Kafka indexing for different data formats.
 * Formats Included:
 * <ul>
 * <li>Avro (with and without schema registry)</li>
 * <li>CSV</li>
 * <li>JSON</li>
 * <li>Protobuf (with and without schema registry)</li>
 * </ul>
 *
 * This tests both InputFormat and Parser. Parser is deprecated for Kafka Ingestion, and those tests will be removed in the future.
 */
public class KafkaDataFormatsTest extends EmbeddedClusterTestBase
{
  private static final long CYCLE_PADDING_MS = 100;
  private static final int EVENTS_PER_SECOND = 6;

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private KafkaResource kafkaServer;
  private KafkaSchemaRegistryResource schemaRegistry;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper().useContainerFriendlyHostname();

    kafkaServer = new KafkaResource();

    schemaRegistry = new KafkaSchemaRegistryResource(kafkaServer);

    indexer.addProperty("druid.segment.handoff.pollDuration", "PT0.1s")
           .addProperty("druid.worker.capacity", "10");
    overlord.addProperty("druid.manager.segments.useIncrementalCache", "ifSynced")
            .addProperty("druid.manager.segments.pollDuration", "PT0.1s");
    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "ifSynced");
    cluster.addExtension(KafkaIndexTaskModule.class)
           .addExtension(ProtobufExtensionsModule.class)
           .addExtension(AvroExtensionsModule.class)
           .useLatchableEmitter()
           .addCommonProperty("druid.monitoring.emissionPeriod", "PT0.1s")
           .addCommonProperty(
               "druid.monitoring.monitors",
               "[\"org.apache.druid.server.metrics.TaskCountStatsMonitor\"]"
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
  @Timeout(30)
  public void test_indexKafka_avroDataFormat_withParser() throws IOException
  {
    kafkaServer.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = overlord.bindings().jsonMapper().readValue("{\"type\": \"avro\"}", EventSerializer.class);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, false);
    String jsonString = "{"
                       + "\"type\": \"avro_stream\","
                       + "\"avroBytesDecoder\": {"
                       + "\"type\": \"schema_inline\","
                       + "\"schema\": {"
                       + "\"namespace\": \"org.apache.druid\","
                       + "\"name\": \"wikipedia\","
                       + "\"type\": \"record\","
                       + "\"fields\": ["
                       + "{ \"name\": \"timestamp\", \"type\": \"string\" },"
                       + "{ \"name\": \"page\", \"type\": \"string\" },"
                       + "{ \"name\": \"language\", \"type\": \"string\" },"
                       + "{ \"name\": \"user\", \"type\": \"string\" },"
                       + "{ \"name\": \"unpatrolled\", \"type\": \"string\" },"
                       + "{ \"name\": \"newPage\", \"type\": \"string\" },"
                       + "{ \"name\": \"robot\", \"type\": \"string\" },"
                       + "{ \"name\": \"anonymous\", \"type\": \"string\" },"
                       + "{ \"name\": \"namespace\", \"type\": \"string\" },"
                       + "{ \"name\": \"continent\", \"type\": \"string\" },"
                       + "{ \"name\": \"country\", \"type\": \"string\" },"
                       + "{ \"name\": \"region\", \"type\": \"string\" },"
                       + "{ \"name\": \"city\", \"type\": \"string\" },"
                       + "{ \"name\": \"added\", \"type\": \"long\" },"
                       + "{ \"name\": \"deleted\", \"type\": \"long\" },"
                       + "{ \"name\": \"delta\", \"type\": \"long\" }"
                       + "]"
                       + "}"
                       + "},"
                       + "\"parseSpec\": {"
                       + "\"format\": \"avro\","
                       + "\"timestampSpec\": {"
                       + "\"column\": \"timestamp\","
                       + "\"format\": \"auto\""
                       + "},"
                       + "\"dimensionsSpec\": {"
                       + "\"dimensions\": [\"page\", \"language\", \"user\", \"unpatrolled\", \"newPage\", \"robot\", \"anonymous\", \"namespace\", \"continent\", \"country\", \"region\", \"city\"],"
                       + "\"dimensionExclusions\": [],"
                       + "\"spatialDimensions\": []"
                       + "}"
                       + "}"
                       + "}";
    Map<String, Object> parserMap = overlord.bindings().jsonMapper().readValue(jsonString, new TypeReference<Map<String, Object>>(){});
    KafkaSupervisorSpec supervisorSpec = createDeprectatedKafkaSupervisor(dataSource, dataSource, parserMap);

    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
  }

  @Test
  @Timeout(30)
  public void test_indexKafka_avroDataFormat() throws IOException
  {
    kafkaServer.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = overlord.bindings().jsonMapper().readValue("{\"type\": \"avro\"}", EventSerializer.class);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, false);
    
    // Create Avro schema as a Map for InlineSchemaAvroBytesDecoder
    Map<String, Object> avroSchema = createWikipediaAvroSchemaMap();
    
    // Create the AvroBytesDecoder with inline schema
    InlineSchemaAvroBytesDecoder avroBytesDecoder = overlord.bindings().jsonMapper().readValue(
        StringUtils.format("{\"type\": \"schema_inline\", \"schema\": %s}", 
                           overlord.bindings().jsonMapper().writeValueAsString(avroSchema)),
        InlineSchemaAvroBytesDecoder.class
    );
    
    // Create JSONPathSpec for field discovery
    JSONPathSpec flattenSpec = new JSONPathSpec(true, null);
    
    // Build AvroStreamInputFormat using constructor
    AvroStreamInputFormat inputFormat = new AvroStreamInputFormat(
        flattenSpec,        // flattenSpec with useFieldDiscovery=true
        avroBytesDecoder,   // avroBytesDecoder 
        false,              // binaryAsString
        null                // extractUnionsByType
    );
    
    KafkaSupervisorSpec supervisorSpec = createKafkaSupervisor(dataSource, dataSource, inputFormat);
    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
  }

  @Test
  @Timeout(30)
  public void test_indexKafka_avroDataFormatWithSchemaRegistry_withParser() throws JsonProcessingException
  {
    kafkaServer.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = new AvroSchemaRegistryEventSerializer(StringUtils.format("%s:%s", cluster.getEmbeddedHostname().toString(), schemaRegistry.getContainer().getMappedPort(9081)));
    serializer.initialize(dataSource);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, true);
    String jsonString = "{"
                       + "\"type\": \"avro_stream\","
                       + "\"avroBytesDecoder\": {"
                       + "\"type\": \"schema_registry\","
                       + "\"url\": \"" + StringUtils.format("http://%s:%s", cluster.getEmbeddedHostname().toString(), schemaRegistry.getContainer().getMappedPort(9081)) + "\""
                       + "},"
                       + "\"parseSpec\": {"
                       + "\"format\": \"avro\","
                       + "\"timestampSpec\": {"
                       + "\"column\": \"timestamp\","
                       + "\"format\": \"auto\""
                       + "},"
                       + "\"dimensionsSpec\": {"
                       + "\"dimensions\": [\"page\", \"language\", \"user\", \"unpatrolled\", \"newPage\", \"robot\", \"anonymous\", \"namespace\", \"continent\", \"country\", \"region\", \"city\"],"
                       + "\"dimensionExclusions\": [],"
                       + "\"spatialDimensions\": []"
                       + "}"
                       + "}"
                       + "}";
    Map<String, Object> parserMap = overlord.bindings().jsonMapper().readValue(jsonString, new TypeReference<Map<String, Object>>(){});
    KafkaSupervisorSpec supervisorSpec = createDeprectatedKafkaSupervisor(dataSource, dataSource, parserMap);

    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
  }

  @Test
  @Timeout(30)
  public void test_indexKafka_avroDataFormatWithSchemaRegistry() throws JsonProcessingException
  {
    kafkaServer.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = new AvroSchemaRegistryEventSerializer(StringUtils.format("%s:%s", cluster.getEmbeddedHostname().toString(), schemaRegistry.getContainer().getMappedPort(9081)));
    serializer.initialize(dataSource);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, true);
    SchemaRegistryBasedAvroBytesDecoder avroBytesDecoder = overlord.bindings().jsonMapper().readValue(
        StringUtils.format("{\"type\": \"schema_registry\", \"urls\": [\"http://%s:%s\"]}",
                           cluster.getEmbeddedHostname().toString(),
                           schemaRegistry.getContainer().getMappedPort(9081)),
        SchemaRegistryBasedAvroBytesDecoder.class
    );
    AvroStreamInputFormat inputFormat = new AvroStreamInputFormat(null, avroBytesDecoder, null, null);

    KafkaSupervisorSpec supervisorSpec = createKafkaSupervisor(dataSource, dataSource, inputFormat);
    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );

    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
  }

  @Test
  @Timeout(30)
  public void test_indexKafka_csvDataFormat() throws JsonProcessingException
  {
    kafkaServer.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = overlord.bindings().jsonMapper().readValue("{\"type\": \"csv\"}", EventSerializer.class);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, false);

    CsvInputFormat inputFormat = new CsvInputFormat(List.of("timestamp", "page", "language", "user", "unpatrolled", "newPage", "robot", "anonymous", "namespace", "continent", "country", "region", "city", "added", "deleted", "delta"), null, null, false, 0, false);
    KafkaSupervisorSpec supervisorSpec = createKafkaSupervisor(dataSource, dataSource, inputFormat);

    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
  }

  @Test
  @Timeout(30)
  public void test_indexKafka_csvDataFormat_withParser() throws JsonProcessingException
  {
    kafkaServer.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = overlord.bindings().jsonMapper().readValue("{\"type\": \"csv\"}", EventSerializer.class);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, false);
    String jsonString = "{"
                       + "\"type\": \"string\","
                       + "\"parseSpec\": {"
                       + "\"format\": \"csv\","
                       + "\"timestampSpec\": {"
                       + "\"column\": \"timestamp\","
                       + "\"format\": \"auto\""
                       + "},"
                       + "\"columns\": [\"timestamp\",\"page\",\"language\",\"user\",\"unpatrolled\",\"newPage\",\"robot\",\"anonymous\",\"namespace\",\"continent\",\"country\",\"region\",\"city\",\"added\",\"deleted\",\"delta\"],"
                       + "\"dimensionsSpec\": {"
                       + "\"dimensions\": [\"page\", \"language\", \"user\", \"unpatrolled\", \"newPage\", \"robot\", \"anonymous\", \"namespace\", \"continent\", \"country\", \"region\", \"city\"],"
                       + "\"dimensionExclusions\": [],"
                       + "\"spatialDimensions\": []"
                       + "}"
                       + "}"
                       + "}";
    Map<String, Object> parserMap = overlord.bindings().jsonMapper().readValue(jsonString, new TypeReference<Map<String, Object>>(){});
    KafkaSupervisorSpec supervisorSpec = createDeprectatedKafkaSupervisor(dataSource, dataSource, parserMap);

    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
  }

  @Test
  @Timeout(30)
  public void test_indexKafka_jsonDataFormat_withParser() throws JsonProcessingException
  {
    kafkaServer.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = overlord.bindings().jsonMapper().readValue("{\"type\": \"json\"}", EventSerializer.class);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, false);
    String jsonString = "{"
                       + "\"type\": \"string\","
                       + "\"parseSpec\": {"
                       + "\"format\": \"json\","
                       + "\"timestampSpec\": {"
                       + "\"column\": \"timestamp\","
                       + "\"format\": \"auto\""
                       + "},"
                       + "\"dimensionsSpec\": {"
                       + "\"dimensions\": [\"page\", \"language\", \"user\", \"unpatrolled\", \"newPage\", \"robot\", \"anonymous\", \"namespace\", \"continent\", \"country\", \"region\", \"city\"],"
                       + "\"dimensionExclusions\": [],"
                       + "\"spatialDimensions\": []"
                       + "}"
                       + "}"
                       + "}";
    Map<String, Object> parserMap = overlord.bindings().jsonMapper().readValue(jsonString, new TypeReference<Map<String, Object>>(){});
    KafkaSupervisorSpec supervisorSpec = createDeprectatedKafkaSupervisor(dataSource, dataSource, parserMap);

    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
  }

  @Test
  @Timeout(30)
  public void test_indexKafka_jsonDataFormat() throws JsonProcessingException
  {
    kafkaServer.createTopicWithPartitions(dataSource, 3);

    EventSerializer serializer = overlord.bindings().jsonMapper().readValue("{\"type\": \"json\"}", EventSerializer.class);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, false);
    InputFormat inputFormat = new JsonInputFormat(null, null, null, false, null, null);
    KafkaSupervisorSpec supervisorSpec = createKafkaSupervisor(dataSource, dataSource, inputFormat);

    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
  }

  @Test
  @Timeout(30)
  public void test_indexKafka_protobufDataFormat_withParser() throws JsonProcessingException
  {
    kafkaServer.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = overlord.bindings().jsonMapper().readValue("{\"type\": \"protobuf\"}", EventSerializer.class);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, false);

    String jsonString = "{"
                       + "\"type\": \"protobuf\","
                       + "\"protoBytesDecoder\": {"
                       + "\"type\": \"file\","
                       + "\"descriptor\": \"data/protobuf/wikipedia.desc\","
                       + "\"protoMessageType\": \"Wikipedia\""
                       + "},"
                       + "\"parseSpec\": {"
                       + "\"format\": \"json\","
                       + "\"timestampSpec\": {"
                       + "\"column\": \"timestamp\","
                       + "\"format\": \"auto\""
                       + "},"
                       + "\"dimensionsSpec\": {"
                       + "\"dimensions\": [\"page\", \"language\", \"user\", \"unpatrolled\", \"newPage\", \"robot\", \"anonymous\", \"namespace\", \"continent\", \"country\", \"region\", \"city\"],"
                       + "\"dimensionExclusions\": [],"
                       + "\"spatialDimensions\": []"
                       + "}"
                       + "}"
                       + "}";
    Map<String, Object> parserMap = overlord.bindings().jsonMapper().readValue(jsonString, new TypeReference<Map<String, Object>>(){});
    KafkaSupervisorSpec supervisorSpec = createDeprectatedKafkaSupervisor(dataSource, dataSource, parserMap);

    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
  }

  @Test
  public void test_indexKafka_protobufDataFormat() throws JsonProcessingException
  {
    kafkaServer.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = overlord.bindings().jsonMapper().readValue("{\"type\": \"protobuf\"}", EventSerializer.class);

    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, false);

    FileBasedProtobufBytesDecoder protobufBytesDecoder = overlord.bindings().jsonMapper().readValue(
        "{\"type\": \"file\", \"protoMessageType\": \"Wikipedia\", \"descriptor\": \"data/protobuf/wikipedia.desc\"}",
        FileBasedProtobufBytesDecoder.class
    );

    ProtobufInputFormat inputFormat = new ProtobufInputFormat(null, protobufBytesDecoder);

    KafkaSupervisorSpec supervisorSpec = createKafkaSupervisor(dataSource, dataSource, inputFormat);

    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
  }

  @Test
  @Timeout(30)
  public void test_indexKafka_protobufDataFormatWithSchemaRegistry_withParser() throws JsonProcessingException
  {
    kafkaServer.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = new ProtobufSchemaRegistryEventSerializer(StringUtils.format("%s:%s", cluster.getEmbeddedHostname().toString(), schemaRegistry.getContainer().getMappedPort(9081)));
    serializer.initialize(dataSource);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, true);
    String jsonString = "{"
                       + "\"type\": \"protobuf\","
                       + "\"protoBytesDecoder\": {"
                       + "\"type\": \"schema_registry\","
                       + "\"url\": \"" + StringUtils.format("http://%s:%s", cluster.getEmbeddedHostname().toString(), schemaRegistry.getContainer().getMappedPort(9081)) + "\""
                       + "},"
                       + "\"parseSpec\": {"
                       + "\"format\": \"json\","
                       + "\"timestampSpec\": {"
                       + "\"column\": \"timestamp\","
                       + "\"format\": \"auto\""
                       + "},"
                       + "\"dimensionsSpec\": {"
                       + "\"dimensions\": [\"page\", \"language\", \"user\", \"unpatrolled\", \"newPage\", \"robot\", \"anonymous\", \"namespace\", \"continent\", \"country\", \"region\", \"city\"],"
                       + "\"dimensionExclusions\": [],"
                       + "\"spatialDimensions\": []"
                       + "}"
                       + "}"
                       + "}";
    Map<String, Object> parserMap = overlord.bindings().jsonMapper().readValue(jsonString, new TypeReference<Map<String, Object>>(){});
    KafkaSupervisorSpec supervisorSpec = createDeprectatedKafkaSupervisor(dataSource, dataSource, parserMap);

    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
  }

  @Test
  @Timeout(30)
  public void test_indexKafka_protobufDataFormatWithSchemaRegistry() throws JsonProcessingException
  {
    kafkaServer.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = new ProtobufSchemaRegistryEventSerializer(StringUtils.format("%s:%s", cluster.getEmbeddedHostname().toString(), schemaRegistry.getContainer().getMappedPort(9081)));
    serializer.initialize(dataSource);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, true);
    SchemaRegistryBasedProtobufBytesDecoder protobufBytesDecoder = overlord.bindings().jsonMapper().readValue(
        StringUtils.format("{\"type\": \"schema_registry\", \"urls\": [\"http://%s:%s\"]}",
            cluster.getEmbeddedHostname().toString(),
            schemaRegistry.getContainer().getMappedPort(9081)),
        SchemaRegistryBasedProtobufBytesDecoder.class
    );
    ProtobufInputFormat inputFormat = new ProtobufInputFormat(null, protobufBytesDecoder);
    KafkaSupervisorSpec supervisorSpec = createKafkaSupervisor(dataSource, dataSource, inputFormat);
    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );

    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
  }

  @Test
  @Timeout(30)
  public void test_indexKafka_tsvDataFormat_withParser() throws JsonProcessingException
  {
    kafkaServer.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = overlord.bindings().jsonMapper().readValue("{\"type\": \"tsv\"}", EventSerializer.class);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, false);
    String jsonString = "{"
                       + "\"type\": \"string\","
                       + "\"parseSpec\": {"
                       + "\"format\": \"tsv\","
                       + "\"timestampSpec\": {"
                       + "\"column\": \"timestamp\","
                       + "\"format\": \"auto\""
                       + "},"
                       + "\"columns\": [\"timestamp\",\"page\",\"language\",\"user\",\"unpatrolled\",\"newPage\",\"robot\",\"anonymous\",\"namespace\",\"continent\",\"country\",\"region\",\"city\",\"added\",\"deleted\",\"delta\"],"
                       + "\"dimensionsSpec\": {"
                       + "\"dimensions\": [\"page\", \"language\", \"user\", \"unpatrolled\", \"newPage\", \"robot\", \"anonymous\", \"namespace\", \"continent\", \"country\", \"region\", \"city\"],"
                       + "\"dimensionExclusions\": [],"
                       + "\"spatialDimensions\": []"
                       + "}"
                       + "}"
                       + "}";
    Map<String, Object> parserMap = overlord.bindings().jsonMapper().readValue(jsonString, new TypeReference<Map<String, Object>>(){});
    KafkaSupervisorSpec supervisorSpec = createDeprectatedKafkaSupervisor(dataSource, dataSource, parserMap);

    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
  }

  @Test
  @Timeout(30)
  public void test_indexKafka_tsvDataFormat() throws JsonProcessingException
  {
    kafkaServer.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = overlord.bindings().jsonMapper().readValue("{\"type\": \"tsv\"}", EventSerializer.class);
    int recordCount = generateStreamAndPublishToKafka(dataSource, serializer, false);
    DelimitedInputFormat inputFormat = new DelimitedInputFormat(
        List.of("timestamp", "page", "language", "user", "unpatrolled", "newPage", "robot", "anonymous",
                "namespace", "continent", "country", "region", "city", "added", "deleted", "delta"),
        null, null, false, false, 0,
        null
    );
    KafkaSupervisorSpec supervisorSpec = createKafkaSupervisor(dataSource, dataSource, inputFormat);

    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(supervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
  }

  private void waitForDataAndVerifyIngestedEvents(String dataSource, int expectedCount)
  {
    // Wait for the task to succeed
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
    Assertions.assertEquals(StringUtils.format("%d", expectedCount), cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
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
          Map.of("schema.registry.url", StringUtils.format("http://%s:%s", cluster.getEmbeddedHostname().toString(), schemaRegistry.getContainer().getMappedPort(9081)))
      );
    } else {
      kafkaServer.produceRecordsToTopic(producerRecords);
    }
    return producerRecords.size();
  }

  // Uses parser which is deprcated for KafkaIndexingService
  private KafkaSupervisorSpec createDeprectatedKafkaSupervisor(String supervisorId, String topic, Map<String, Object> parserMap)
  {
    return new KafkaSupervisorSpec(
        supervisorId,
        null,
        DataSchema.builder()
                  .withDataSource(dataSource)
                  .withTimestamp(new TimestampSpec("timestamp", null, null))
                  .withDimensions(DimensionsSpec.EMPTY)
                  .withParserMap(parserMap)
                  .build(),
        createTuningConfig(),
        new KafkaSupervisorIOConfig(
            topic,
            null,
            null,
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

  private Map<String, Object> createWikipediaAvroSchemaMap()
  {
    Map<String, Object> schema = new HashMap<>();
    schema.put("namespace", "org.apache.druid");
    schema.put("name", "wikipedia");
    schema.put("type", "record");
    
    List<Map<String, Object>> fields = new ArrayList<>();
    fields.add(Map.of("name", "timestamp", "type", "string"));
    fields.add(Map.of("name", "page", "type", "string"));
    fields.add(Map.of("name", "language", "type", "string"));
    fields.add(Map.of("name", "user", "type", "string"));
    fields.add(Map.of("name", "unpatrolled", "type", "string"));
    fields.add(Map.of("name", "newPage", "type", "string"));
    fields.add(Map.of("name", "robot", "type", "string"));
    fields.add(Map.of("name", "anonymous", "type", "string"));
    fields.add(Map.of("name", "namespace", "type", "string"));
    fields.add(Map.of("name", "continent", "type", "string"));
    fields.add(Map.of("name", "country", "type", "string"));
    fields.add(Map.of("name", "region", "type", "string"));
    fields.add(Map.of("name", "city", "type", "string"));
    fields.add(Map.of("name", "added", "type", "long"));
    fields.add(Map.of("name", "deleted", "type", "long"));
    fields.add(Map.of("name", "delta", "type", "long"));
    
    schema.put("fields", fields);
    return schema;
  }

}
