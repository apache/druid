/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.druid.transform;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactory;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.segment.transform.TransformSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EnrichResourceNameTransformTest
{
  @Mock
  private LookupExtractorFactoryContainerProvider mockLookupProvider;

  @Mock
  private LookupExtractorFactoryContainer mockLookupContainer;

  @Mock
  private LookupExtractorFactory mockLookupExtractorFactory;

  @Mock
  private LookupExtractor mockLookupExtractor;

  private static final MapInputRowParser PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
        new TimestampSpec("t", "auto", DateTimes.of("2020-01-01")),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("metric_name", "connector_id", "tenant", "logical_cluster_id", "compute_pool")))
      )
  );

  @Before
  public void setUp()
  {
    // Setup mock lookup behavior
    when(mockLookupProvider.get("resource_display_name_test_lookup"))
        .thenReturn(Optional.of(mockLookupContainer));
    when(mockLookupContainer.getLookupExtractorFactory())
        .thenReturn(mockLookupExtractorFactory);

    // Setup lookup data
    when(mockLookupExtractor.apply("lkc-abc123")).thenReturn("My Kafka Cluster");
    when(mockLookupExtractor.apply("lcc-xyz789")).thenReturn("My Connect Cluster");
    when(mockLookupExtractor.apply("lsrc-def456")).thenReturn("My Schema Registry");
    when(mockLookupExtractor.apply("ksql-ghi789")).thenReturn("My KSQL Cluster");
    when(mockLookupExtractor.apply("fcp-jkl012")).thenReturn("My Flink Compute Pool");
  }

  @Test
  public void testKafkaMetricEnrichment()
  {
    when(mockLookupExtractorFactory.get()).thenReturn(mockLookupExtractor);

    EnrichResourceNameTransform transform = new EnrichResourceNameTransform(
        "resource_name",
        "metric_name",
        ImmutableSet.of("kafka-", "kafka_"),
        "kafka_resource",
        "kafka_resource_derived",
        ImmutableSet.of(),
        "tableflow_resource",
        ImmutableSet.of(),
        "connect_resource",
        ImmutableSet.of(),
        "ksql_resource",
        ImmutableSet.of(),
        "schema_registry_resource",
        ImmutableSet.of(),
        "fcp_resource",
        "resource_display_name_test_lookup",
        mockLookupProvider
    );

    TransformSpec transformSpec = new TransformSpec(null, ImmutableList.of(transform));
    InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);

    // Test Kafka metric with tenant lookup
    Map<String, Object> kafkaRowData = ImmutableMap.<String, Object>builder()
        .put("metric_name", "kafka-producer-metrics")
        .put("kafka_resource", "lkc-abc123")
        .put("kafka_resource_derived", "lkc-abc123_topic1")
        .build();

    InputRow kafkaRow = parser.parseBatch(kafkaRowData).get(0);
    Assert.assertNotNull(kafkaRow);
    Assert.assertEquals("My Kafka Cluster", kafkaRow.getRaw("resource_name"));
  }


  @Test
  public void testKafkaMetricWithDerivedResource()
  {
    when(mockLookupExtractorFactory.get()).thenReturn(mockLookupExtractor);

    EnrichResourceNameTransform transform = new EnrichResourceNameTransform(
            "resource_name",
            "metric_name",
            ImmutableSet.of("kafka-"),
            "kafka_resource",
            "kafka_resource_derived",
            ImmutableSet.of(),
            "tableflow_resource",
            ImmutableSet.of(),
            "connect_resource",
            ImmutableSet.of(),
            "ksql_resource",
            ImmutableSet.of(),
            "schema_registry_resource",
            ImmutableSet.of(),
            "fcp_resource",
            "resource_display_name_test_lookup",
            mockLookupProvider
    );

    TransformSpec transformSpec = new TransformSpec(null, ImmutableList.of(transform));
    InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);

    // Test Kafka metric with only derived resource ID (kafka_resource is null)
    Map<String, Object> kafkaRowData = ImmutableMap.<String, Object>builder()
            .put("metric_name", "kafka-producer-metrics")
            .put("kafka_resource_derived", "lkc-abc123_topic1")
            .build();

    InputRow kafkaRow = parser.parseBatch(kafkaRowData).get(0);
    Assert.assertNotNull(kafkaRow);
    Assert.assertEquals("My Kafka Cluster", kafkaRow.getRaw("resource_name"));
  }

  @Test
  public void testConnectMetricEnrichment()
  {
    when(mockLookupExtractorFactory.get()).thenReturn(mockLookupExtractor);

    EnrichResourceNameTransform transform = new EnrichResourceNameTransform(
        "resource_name",
        "metric_name",
        ImmutableSet.of(),
        "kafka_resource",
        "kafka_resource_derived",
        ImmutableSet.of(),
        "tableflow_resource",
        ImmutableSet.of("connect-", "kafka-connect-"),
        "connect_resource",
        ImmutableSet.of(),
        "ksql_resource",
        ImmutableSet.of(),
        "schema_registry_resource",
        ImmutableSet.of(),
        "fcp_resource",
        "resource_display_name_test_lookup",
        mockLookupProvider
    );

    TransformSpec transformSpec = new TransformSpec(null, ImmutableList.of(transform));
    InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);

    // Test Connect metric with connector_id lookup
    Map<String, Object> connectRowData = ImmutableMap.<String, Object>builder()
        .put("metric_name", "connect-kafka-source-metrics")
        .put("connect_resource", "lcc-xyz789")
        .build();

    InputRow connectRow = parser.parseBatch(connectRowData).get(0);
    Assert.assertNotNull(connectRow);
    Assert.assertEquals("My Connect Cluster", connectRow.getRaw("resource_name"));
  }

  @Test
  public void testSchemaRegistryMetricEnrichment()
  {
    when(mockLookupExtractorFactory.get()).thenReturn(mockLookupExtractor);

    EnrichResourceNameTransform transform = new EnrichResourceNameTransform(
        "resource_name",
        "metric_name",
        ImmutableSet.of(),
        "kafka_resource",
        "kafka_resource_derived",
        ImmutableSet.of(),
        "tableflow_resource",
        ImmutableSet.of(),
        "connect_resource",
        ImmutableSet.of(),
        "ksql_resource",
        ImmutableSet.of("schema_registry-"),
        "schema_registry_resource",
        ImmutableSet.of(),
        "fcp_resource",
        "resource_display_name_test_lookup",
        mockLookupProvider
    );

    TransformSpec transformSpec = new TransformSpec(null, ImmutableList.of(transform));
    InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);

    // Test Schema Registry metric with tenant lookup
    Map<String, Object> schemaRegistryRowData = ImmutableMap.<String, Object>builder()
        .put("metric_name", "schema_registry-subject-metrics")
        .put("schema_registry_resource", "lsrc-def456")
        .build();

    InputRow schemaRegistryRow = parser.parseBatch(schemaRegistryRowData).get(0);
    Assert.assertNotNull(schemaRegistryRow);
    Assert.assertEquals("My Schema Registry", schemaRegistryRow.getRaw("resource_name"));
  }

  @Test
  public void testKSQLMetricEnrichment()
  {
    when(mockLookupExtractorFactory.get()).thenReturn(mockLookupExtractor);

    EnrichResourceNameTransform transform = new EnrichResourceNameTransform(
        "resource_name",
        "metric_name",
        ImmutableSet.of(),
        "kafka_resource",
        "kafka_resource_derived",
        ImmutableSet.of(),
        "tableflow_resource",
        ImmutableSet.of(),
        "connect_resource",
        ImmutableSet.of("ksql-"),
        "ksql_resource",
        ImmutableSet.of(),
        "schema_registry_resource",
        ImmutableSet.of(),
        "fcp_resource",
        "resource_display_name_test_lookup",
        mockLookupProvider
    );

    TransformSpec transformSpec = new TransformSpec(null, ImmutableList.of(transform));
    InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);

    // Test KSQL metric with logical_cluster_id lookup
    Map<String, Object> ksqlRowData = ImmutableMap.<String, Object>builder()
        .put("metric_name", "ksql-query-metrics")
        .put("ksql_resource", "ksql-ghi789")
        .build();

    InputRow ksqlRow = parser.parseBatch(ksqlRowData).get(0);
    Assert.assertNotNull(ksqlRow);
    Assert.assertEquals("My KSQL Cluster", ksqlRow.getRaw("resource_name"));
  }

  @Test
  public void testFCPMetricEnrichment()
  {
    when(mockLookupExtractorFactory.get()).thenReturn(mockLookupExtractor);

    EnrichResourceNameTransform transform = new EnrichResourceNameTransform(
        "resource_name",
        "metric_name",
        ImmutableSet.of(),
        "kafka_resource",
        "kafka_resource_derived",
        ImmutableSet.of(),
        "tableflow_resource",
        ImmutableSet.of(),
        "connect_resource",
        ImmutableSet.of(),
        "ksql_resource",
        ImmutableSet.of(),
        "schema_registry_resource",
        ImmutableSet.of("fcp-"),
        "fcp_resource",
        "resource_display_name_test_lookup",
        mockLookupProvider
    );

    TransformSpec transformSpec = new TransformSpec(null, ImmutableList.of(transform));
    InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);

    // Test FCP metric with compute_pool lookup
    Map<String, Object> fcpRowData = ImmutableMap.<String, Object>builder()
        .put("metric_name", "fcp-compute-metrics")
        .put("fcp_resource", "fcp-jkl012")
        .build();

    InputRow fcpRow = parser.parseBatch(fcpRowData).get(0);
    Assert.assertNotNull(fcpRow);
    Assert.assertEquals("My Flink Compute Pool", fcpRow.getRaw("resource_name"));
  }

  @Test
  public void testNoPrefixMatch()
  {
    EnrichResourceNameTransform transform = new EnrichResourceNameTransform(
        "resource_name",
        "metric_name",
        ImmutableSet.of("kafka-"),
        "kafka_resource",
        "kafka_resource_derived",
        ImmutableSet.of(),
        "tableflow_resource",
        ImmutableSet.of(),
        "connect_resource",
        ImmutableSet.of(),
        "ksql_resource",
        ImmutableSet.of(),
        "schema_registry_resource",
        ImmutableSet.of(),
        "fcp_resource",
        "resource_display_name_test_lookup",
        mockLookupProvider
    );

    TransformSpec transformSpec = new TransformSpec(null, ImmutableList.of(transform));
    InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);

    // Test metric that doesn't match any prefix
    Map<String, Object> rowData = ImmutableMap.<String, Object>builder()
        .put("metric_name", "unknown-metrics")
        .put("kafka_resource", "lkc-abc123")
        .build();

    InputRow row = parser.parseBatch(rowData).get(0);
    Assert.assertNotNull(row);
    Assert.assertNull(row.getRaw("resource_name"));
  }

  @Test
  public void testLookupNotFound()
  {
    when(mockLookupProvider.get("nonexistent_lookup"))
        .thenReturn(Optional.empty());

    EnrichResourceNameTransform transform = new EnrichResourceNameTransform(
        "resource_name",
        "metric_name",
        ImmutableSet.of("kafka-"),
        "kafka_resource",
        "kafka_resource_derived",
        ImmutableSet.of(),
        "tableflow_resource",
        ImmutableSet.of(),
        "connect_resource",
        ImmutableSet.of(),
        "ksql_resource",
        ImmutableSet.of(),
        "schema_registry_resource",
        ImmutableSet.of(),
        "fcp_resource",
        "nonexistent_lookup",
        mockLookupProvider
    );

    TransformSpec transformSpec = new TransformSpec(null, ImmutableList.of(transform));
    InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);

    // Test with nonexistent lookup
    Map<String, Object> rowData = ImmutableMap.<String, Object>builder()
        .put("metric_name", "kafka-metrics")
        .put("kafka_resource", "lkc-abc123")
        .build();

    InputRow row = parser.parseBatch(rowData).get(0);
    Assert.assertNotNull(row);
    Assert.assertNull(row.getRaw("resource_name"));
  }

  @Test
  public void testLookupReturnsNull()
  {
    when(mockLookupExtractorFactory.get()).thenReturn(mockLookupExtractor);

    EnrichResourceNameTransform transform = new EnrichResourceNameTransform(
        "resource_name",
        "metric_name",
        ImmutableSet.of("kafka-"),
        "kafka_resource",
        "kafka_resource_derived",
        ImmutableSet.of(),
        "tableflow_resource",
        ImmutableSet.of(),
        "connect_resource",
        ImmutableSet.of(),
        "ksql_resource",
        ImmutableSet.of(),
        "schema_registry_resource",
        ImmutableSet.of(),
        "fcp_resource",
        "resource_display_name_test_lookup",
        mockLookupProvider
    );

    TransformSpec transformSpec = new TransformSpec(null, ImmutableList.of(transform));
    InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);

    // Test with resource ID that doesn't exist in lookup
    Map<String, Object> rowData = ImmutableMap.<String, Object>builder()
        .put("metric_name", "kafka-metrics")
        .put("kafka_resource", "unknown-id")
        .build();

    InputRow row = parser.parseBatch(rowData).get(0);
    Assert.assertNotNull(row);
    Assert.assertNull(row.getRaw("resource_name"));
  }

  @Test
  public void testMultiplePrefixMatching()
  {
    when(mockLookupExtractorFactory.get()).thenReturn(mockLookupExtractor);

    EnrichResourceNameTransform transform = new EnrichResourceNameTransform(
        "resource_name",
        "metric_name",
        ImmutableSet.of("kafka-", "kafka_"),
        "kafka_resource",
        "kafka_resource_derived",
        ImmutableSet.of(),
        "tableflow_resource",
        ImmutableSet.of(),
        "connect_resource",
        ImmutableSet.of(),
        "ksql_resource",
        ImmutableSet.of(),
        "schema_registry_resource",
        ImmutableSet.of(),
        "fcp_resource",
        "resource_display_name_test_lookup",
        mockLookupProvider
    );

    TransformSpec transformSpec = new TransformSpec(null, ImmutableList.of(transform));
    InputRowParser<Map<String, Object>> parser = transformSpec.decorate(PARSER);

    // Test both kafka- and kafka_ prefixes
    Map<String, Object> kafkaDashData = ImmutableMap.<String, Object>builder()
        .put("metric_name", "kafka-producer-metrics")
        .put("kafka_resource", "lkc-abc123")
        .build();

    InputRow kafkaDashRow = parser.parseBatch(kafkaDashData).get(0);
    Assert.assertNotNull(kafkaDashRow);
    Assert.assertEquals("My Kafka Cluster", kafkaDashRow.getRaw("resource_name"));

    Map<String, Object> kafkaUnderscoreData = ImmutableMap.<String, Object>builder()
        .put("metric_name", "kafka_consumer-metrics")
        .put("kafka_resource", "lkc-abc123")
        .build();

    InputRow kafkaUnderscoreRow = parser.parseBatch(kafkaUnderscoreData).get(0);
    Assert.assertNotNull(kafkaUnderscoreRow);
    Assert.assertEquals("My Kafka Cluster", kafkaUnderscoreRow.getRaw("resource_name"));
  }

  @Test
  public void testGetRequiredColumns()
  {
    EnrichResourceNameTransform transform = new EnrichResourceNameTransform(
        "resource_name",
        "metric_name",
        ImmutableSet.of("kafka-"),
        "kafka_resource",
        "kafka_resource_derived",
        ImmutableSet.of(),
        "tableflow_resource",
        ImmutableSet.of(),
        "connect_resource",
        ImmutableSet.of(),
        "ksql_resource",
        ImmutableSet.of(),
        "schema_registry_resource",
        ImmutableSet.of(),
        "fcp_resource",
        "resource_display_name_test_lookup",
        mockLookupProvider
    );

    Set<String> requiredColumns = transform.getRequiredColumns();
    Assert.assertTrue(requiredColumns.contains("resource_name"));
    Assert.assertTrue(requiredColumns.contains("metric_name"));
    Assert.assertTrue(requiredColumns.contains("kafka_resource"));
    Assert.assertTrue(requiredColumns.contains("tableflow_resource"));
    Assert.assertTrue(requiredColumns.contains("connect_resource"));
    Assert.assertTrue(requiredColumns.contains("ksql_resource"));
    Assert.assertTrue(requiredColumns.contains("schema_registry_resource"));
    Assert.assertTrue(requiredColumns.contains("fcp_resource"));
  }
}
