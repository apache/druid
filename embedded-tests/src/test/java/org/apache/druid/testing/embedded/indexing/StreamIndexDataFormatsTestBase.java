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

import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.avro.AvroExtensionsModule;
import org.apache.druid.data.input.avro.AvroStreamInputFormat;
import org.apache.druid.data.input.avro.InlineSchemaAvroBytesDecoder;
import org.apache.druid.data.input.avro.SchemaRegistryBasedAvroBytesDecoder;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.protobuf.FileBasedProtobufBytesDecoder;
import org.apache.druid.data.input.protobuf.ProtobufExtensionsModule;
import org.apache.druid.data.input.protobuf.ProtobufInputFormat;
import org.apache.druid.data.input.protobuf.SchemaRegistryBasedProtobufBytesDecoder;
import org.apache.druid.data.input.thrift.ThriftExtensionsModule;
import org.apache.druid.data.input.thrift.ThriftInputFormat;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.metadata.Metric;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.StreamIngestResource;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.tools.AvroEventSerializer;
import org.apache.druid.testing.embedded.tools.AvroSchemaRegistryEventSerializer;
import org.apache.druid.testing.embedded.tools.CsvEventSerializer;
import org.apache.druid.testing.embedded.tools.DelimitedEventSerializer;
import org.apache.druid.testing.embedded.tools.EventSerializer;
import org.apache.druid.testing.embedded.tools.JsonEventSerializer;
import org.apache.druid.testing.embedded.tools.ProtobufEventSerializer;
import org.apache.druid.testing.embedded.tools.ProtobufSchemaRegistryEventSerializer;
import org.apache.druid.testing.embedded.tools.StreamGenerator;
import org.apache.druid.testing.embedded.tools.ThriftEventSerializer;
import org.apache.druid.testing.embedded.tools.WikipediaStreamEventStreamGenerator;
import org.apache.druid.testing.embedded.tools.WikipediaThriftEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Embedded test to verify streaming ingestion with different input data formats.
 * Formats Included:
 * <ul>
 * <li>Avro (with and without schema registry)</li>
 * <li>CSV</li>
 * <li>JSON</li>
 * <li>Protobuf (with and without schema registry)</li>
 * <li>Thrift</li>
 * <li>TSV</li>
 * </ul>
 */
public abstract class StreamIndexDataFormatsTestBase extends EmbeddedClusterTestBase
{
  private static final long CYCLE_PADDING_MS = 100;
  private static final int EVENTS_PER_SECOND = 6;
  private static final List<String> WIKI_DIM_LIST = List.of(
      "timestamp",
      "page",
      "language",
      "user",
      "unpatrolled",
      "newPage",
      "robot",
      "anonymous",
      "namespace",
      "continent",
      "country",
      "region",
      "city",
      "added",
      "deleted",
      "delta"
  );

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  private StreamIngestResource<?> streamResource;
  private SchemaRegistryResource schemaRegistry;

  protected abstract StreamIngestResource<?> getStreamResource();

  protected abstract SupervisorSpec createSupervisor(
      String dataSource,
      String topic,
      InputFormat inputFormat
  );

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    streamResource = getStreamResource();
    schemaRegistry = new SchemaRegistryResource(streamResource);

    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper().useContainerFriendlyHostname();

    indexer.addProperty("druid.segment.handoff.pollDuration", "PT0.1s")
           .addProperty("druid.worker.capacity", "10");
    overlord.addProperty("druid.manager.segments.useIncrementalCache", "ifSynced")
            .addProperty("druid.manager.segments.pollDuration", "PT0.1s");
    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "ifSynced");
    cluster.addExtension(ProtobufExtensionsModule.class)
           .addExtension(AvroExtensionsModule.class)
           .addExtension(ThriftExtensionsModule.class)
           .useLatchableEmitter()
           .addCommonProperty("druid.monitoring.emissionPeriod", "PT0.1s")
           .addResource(streamResource)
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
  public void test_avroDataFormat() throws Exception
  {
    streamResource.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = new AvroEventSerializer();
    int recordCount = generateStreamAndPublish(dataSource, serializer, false);
    
    Map<String, Object> avroSchema = createWikipediaAvroSchemaMap();
    
    InlineSchemaAvroBytesDecoder avroBytesDecoder = new InlineSchemaAvroBytesDecoder(
        overlord.bindings().jsonMapper(),
        avroSchema
    );
    
    JSONPathSpec flattenSpec = new JSONPathSpec(true, null);
    
    AvroStreamInputFormat inputFormat = new AvroStreamInputFormat(
        flattenSpec,
        avroBytesDecoder,
        false,
        null
    );
    
    SupervisorSpec supervisorSpec = createSupervisor(dataSource, dataSource, inputFormat);
    final String supervisorId = cluster.callApi().postSupervisor(supervisorSpec);
    Assertions.assertEquals(dataSource, supervisorId);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
    stopSupervisor(supervisorSpec);
  }

  @Test
  @Timeout(30)
  public void test_avroDataFormatWithSchemaRegistry()
  {
    streamResource.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = new AvroSchemaRegistryEventSerializer(schemaRegistry.getHostandPort());
    serializer.initialize(dataSource);
    int recordCount = generateStreamAndPublish(dataSource, serializer, true);
    SchemaRegistryBasedAvroBytesDecoder avroBytesDecoder = new SchemaRegistryBasedAvroBytesDecoder(
        null,
        null,
        List.of(schemaRegistry.getConnectURI()),
        null,
        null,
        overlord.bindings().jsonMapper()
    );
    AvroStreamInputFormat inputFormat = new AvroStreamInputFormat(null, avroBytesDecoder, null, null);

    SupervisorSpec supervisorSpec = createSupervisor(dataSource, dataSource, inputFormat);
    final String supervisorId = cluster.callApi().postSupervisor(supervisorSpec);
    Assertions.assertEquals(dataSource, supervisorId);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
    stopSupervisor(supervisorSpec);
  }

  @Test
  @Timeout(30)
  public void test_csvDataFormat()
  {
    streamResource.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = new CsvEventSerializer();
    int recordCount = generateStreamAndPublish(dataSource, serializer, false);

    CsvInputFormat inputFormat = new CsvInputFormat(WIKI_DIM_LIST, null, null, false, 0, false);
    SupervisorSpec supervisorSpec = createSupervisor(dataSource, dataSource, inputFormat);

    final String supervisorId = cluster.callApi().postSupervisor(supervisorSpec);
    Assertions.assertEquals(dataSource, supervisorId);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
    stopSupervisor(supervisorSpec);
  }

  @Test
  @Timeout(30)
  public void test_jsonDataFormat()
  {
    streamResource.createTopicWithPartitions(dataSource, 3);

    EventSerializer serializer = new JsonEventSerializer(overlord.bindings().jsonMapper());
    int recordCount = generateStreamAndPublish(dataSource, serializer, false);
    InputFormat inputFormat = new JsonInputFormat(null, null, null, false, null, null);
    SupervisorSpec supervisorSpec = createSupervisor(dataSource, dataSource, inputFormat);

    final String supervisorId = cluster.callApi().postSupervisor(supervisorSpec);
    Assertions.assertEquals(dataSource, supervisorId);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
    stopSupervisor(supervisorSpec);
  }

  @Test
  public void test_protobufDataFormat()
  {
    streamResource.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = new ProtobufEventSerializer();

    int recordCount = generateStreamAndPublish(dataSource, serializer, false);

    FileBasedProtobufBytesDecoder protobufBytesDecoder = new FileBasedProtobufBytesDecoder(
        MoreResources.ProbufData.WIKI_PROTOBUF_BYTES_DECODER_RESOURCE,
        MoreResources.ProbufData.WIKI_PROTO_MESSAGE_TYPE
    );

    ProtobufInputFormat inputFormat = new ProtobufInputFormat(null, protobufBytesDecoder);

    SupervisorSpec supervisorSpec = createSupervisor(dataSource, dataSource, inputFormat);

    final String supervisorId = cluster.callApi().postSupervisor(supervisorSpec);
    Assertions.assertEquals(dataSource, supervisorId);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
    stopSupervisor(supervisorSpec);
  }

  @Test
  @Timeout(30)
  public void test_protobufDataFormatWithSchemaRegistry()
  {
    streamResource.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = new ProtobufSchemaRegistryEventSerializer(schemaRegistry.getHostandPort());
    serializer.initialize(dataSource);
    int recordCount = generateStreamAndPublish(dataSource, serializer, true);
    SchemaRegistryBasedProtobufBytesDecoder protobufBytesDecoder = new SchemaRegistryBasedProtobufBytesDecoder(
        null,
        null,
        List.of(schemaRegistry.getConnectURI()),
        null,
        null,
        overlord.bindings().jsonMapper()
    );
    ProtobufInputFormat inputFormat = new ProtobufInputFormat(null, protobufBytesDecoder);
    SupervisorSpec supervisorSpec = createSupervisor(dataSource, dataSource, inputFormat);
    final String supervisorId = cluster.callApi().postSupervisor(supervisorSpec);
    Assertions.assertEquals(dataSource, supervisorId);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
    stopSupervisor(supervisorSpec);
  }

  @Test
  @Timeout(30)
  public void test_tsvDataFormat()
  {
    streamResource.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = new DelimitedEventSerializer();
    int recordCount = generateStreamAndPublish(dataSource, serializer, false);
    DelimitedInputFormat inputFormat = new DelimitedInputFormat(
        WIKI_DIM_LIST,
        null,
        null,
        false,
        false,
        0,
        null
    );
    SupervisorSpec supervisorSpec = createSupervisor(dataSource, dataSource, inputFormat);

    final String supervisorId = cluster.callApi().postSupervisor(supervisorSpec);
    Assertions.assertEquals(dataSource, supervisorId);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
    stopSupervisor(supervisorSpec);
  }

  @Test
  @Timeout(30)
  public void test_thriftDataFormat()
  {
    streamResource.createTopicWithPartitions(dataSource, 3);
    EventSerializer serializer = new ThriftEventSerializer();
    int recordCount = generateStreamAndPublish(dataSource, serializer, false);

    ThriftInputFormat inputFormat = new ThriftInputFormat(
        new JSONPathSpec(true, null),
        null,
        WikipediaThriftEvent.class.getName()
    );

    SupervisorSpec supervisorSpec = createSupervisor(dataSource, dataSource, inputFormat);
    final String supervisorId = cluster.callApi().postSupervisor(supervisorSpec);
    Assertions.assertEquals(dataSource, supervisorId);

    waitForDataAndVerifyIngestedEvents(dataSource, recordCount);
    stopSupervisor(supervisorSpec);
  }

  private void waitForDataAndVerifyIngestedEvents(String dataSource, int expectedCount)
  {
    // Wait for the task to succeed
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/run/time")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(1)
    );
    // Wait for the schema cache to refresh for the datasource under test
    broker.latchableEmitter().waitForEvent(
        event -> event.hasMetricName(Metric.SCHEMA_ROW_SIGNATURE_COLUMN_COUNT)
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );

    // Verify the count of rows ingested into the datasource so far
    Assertions.assertEquals(String.valueOf(expectedCount), cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
  }

  private int generateStreamAndPublish(String topic, EventSerializer serializer, boolean useSchemaRegistry)
  {
    final StreamGenerator streamGenerator = new WikipediaStreamEventStreamGenerator(
        serializer,
        EVENTS_PER_SECOND,
        CYCLE_PADDING_MS
    );
    List<byte[]> records = streamGenerator.generateEvents(10);

    if (useSchemaRegistry) {
      streamResource.publishRecordsToTopic(
          topic,
          records,
          Map.of("schema.registry.url", schemaRegistry.getConnectURI())
      );
    } else {
      streamResource.publishRecordsToTopic(topic, records);
    }
    return records.size();
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

  private DimensionsSpec createWikipediaDimensionsSpec()
  {
    return DimensionsSpec.builder().setDefaultSchemaDimensions(
        WIKI_DIM_LIST
    ).build();
  }

  private void stopSupervisor(SupervisorSpec supervisorSpec)
  {
    cluster.callApi().postSupervisor(supervisorSpec.createSuspendedSpec());
  }
}
