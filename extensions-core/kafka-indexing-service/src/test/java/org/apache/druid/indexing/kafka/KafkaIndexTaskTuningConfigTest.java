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

package org.apache.druid.indexing.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorTuningConfig;
import org.apache.druid.indexing.kafka.supervisor.KafkaTuningConfigBuilder;
import org.apache.druid.indexing.kafka.test.TestModifiedKafkaIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.DimensionValueSetPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.indexing.TuningConfig;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class KafkaIndexTaskTuningConfigTest
{
  private final ObjectMapper mapper;

  public KafkaIndexTaskTuningConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules(new KafkaIndexTaskModule().getJacksonModules());
  }

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String jsonStr = "{\"type\": \"kafka\"}";

    KafkaIndexTaskTuningConfig config = (KafkaIndexTaskTuningConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                TuningConfig.class
            )
        ),
        TuningConfig.class
    );

    Assertions.assertNull(config.getBasePersistDirectory());
    Assertions.assertEquals(new OnheapIncrementalIndex.Spec(), config.getAppendableIndexSpec());
    Assertions.assertEquals(150000, config.getMaxRowsInMemory());
    Assertions.assertEquals(5_000_000, config.getMaxRowsPerSegment().intValue());
    Assertions.assertNull(config.getMaxTotalRows());
    Assertions.assertEquals(new Period("PT10M"), config.getIntermediatePersistPeriod());
    Assertions.assertEquals(0, config.getMaxPendingPersists());
    Assertions.assertEquals(IndexSpec.getDefault(), config.getIndexSpec());
    Assertions.assertEquals(IndexSpec.getDefault(), config.getIndexSpecForIntermediatePersists());
    Assertions.assertFalse(config.isReportParseExceptions());
    Assertions.assertEquals(Duration.ofMinutes(15).toMillis(), config.getHandoffConditionTimeout());
    Assertions.assertEquals(1, config.getNumPersistThreads());
    Assertions.assertEquals(-1, config.getMaxColumnsToMerge());
  }

  @Test
  public void testSerdeWithNonDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"basePersistDirectory\": \"/tmp/xxx\",\n"
                     + "  \"maxRowsInMemory\": 100,\n"
                     + "  \"maxRowsPerSegment\": 100,\n"
                     + "  \"maxTotalRows\": 1000,\n"
                     + "  \"intermediatePersistPeriod\": \"PT1H\",\n"
                     + "  \"maxPendingPersists\": 100,\n"
                     + "  \"reportParseExceptions\": true,\n"
                     + "  \"handoffConditionTimeout\": 100,\n"
                     + "  \"indexSpec\": { \"metricCompression\" : \"NONE\" },\n"
                     + "  \"indexSpecForIntermediatePersists\": { \"dimensionCompression\" : \"uncompressed\" },\n"
                     + "  \"appendableIndexSpec\": { \"type\" : \"onheap\" },\n"
                     + "  \"numPersistThreads\": 2\n"
                     + "}";

    KafkaIndexTaskTuningConfig config = (KafkaIndexTaskTuningConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                TuningConfig.class
            )
        ),
        TuningConfig.class
    );

    Assertions.assertNull(config.getBasePersistDirectory());
    Assertions.assertEquals(new OnheapIncrementalIndex.Spec(), config.getAppendableIndexSpec());
    Assertions.assertEquals(100, config.getMaxRowsInMemory());
    Assertions.assertEquals(100, config.getMaxRowsPerSegment().intValue());
    Assertions.assertNotEquals(null, config.getMaxTotalRows());
    Assertions.assertEquals(1000, config.getMaxTotalRows().longValue());
    Assertions.assertEquals(new Period("PT1H"), config.getIntermediatePersistPeriod());
    Assertions.assertEquals(100, config.getMaxPendingPersists());
    Assertions.assertEquals(true, config.isReportParseExceptions());
    Assertions.assertEquals(100, config.getHandoffConditionTimeout());
    Assertions.assertEquals(
        IndexSpec.builder().withMetricCompression(CompressionStrategy.NONE).build(),
        config.getIndexSpec()
    );
    Assertions.assertEquals(
        IndexSpec.builder().withDimensionCompression(CompressionStrategy.UNCOMPRESSED).build(),
        config.getIndexSpecForIntermediatePersists()
    );
    Assertions.assertEquals(2, config.getNumPersistThreads());
    Assertions.assertEquals(-1, config.getMaxColumnsToMerge());
  }

  @Test
  public void testSerdeWithStreamingPartitionsSpec() throws Exception
  {
    final String jsonStr = "{\n"
                           + "  \"type\": \"kafka\",\n"
                           + "  \"streamingPartitionsSpec\": {\"partitionDimensions\": [\"tenant\", \"region\"]}\n"
                           + "}";

    final KafkaIndexTaskTuningConfig config = (KafkaIndexTaskTuningConfig) mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(jsonStr, TuningConfig.class)),
        TuningConfig.class
    );

    Assertions.assertEquals(
        new DimensionValueSetPartitionsSpec(List.of("tenant", "region")),
        config.getStreamingPartitionsSpec()
    );
    Assertions.assertEquals(List.of("tenant", "region"), partitionDimensionsOf(config));
  }

  @Test
  public void testSerdeWithoutStreamingPartitionsSpecIsNull() throws Exception
  {
    final KafkaIndexTaskTuningConfig config = (KafkaIndexTaskTuningConfig) mapper.readValue(
        mapper.writeValueAsString(mapper.readValue("{\"type\": \"kafka\"}", TuningConfig.class)),
        TuningConfig.class
    );
    Assertions.assertNull(config.getStreamingPartitionsSpec());
  }

  @Test
  public void testSerdeWithEmptyPartitionDimensions() throws Exception
  {
    final KafkaIndexTaskTuningConfig config = roundTripWithStreamingPartitionsSpec("[]");
    Assertions.assertEquals(Collections.emptyList(), partitionDimensionsOf(config));
  }

  @Test
  public void testSerdeWithNullPartitionDimensionsCoalescesToEmpty() throws Exception
  {
    final KafkaIndexTaskTuningConfig config = roundTripWithStreamingPartitionsSpec("null");
    Assertions.assertEquals(Collections.emptyList(), partitionDimensionsOf(config));
  }

  @Test
  public void testSerdeWithEmptyStringPartitionDimension() throws Exception
  {
    // An empty-string dimension name is preserved verbatim (it simply never matches an ingested value).
    final KafkaIndexTaskTuningConfig config = roundTripWithStreamingPartitionsSpec("[\"\"]");
    Assertions.assertEquals(List.of(""), partitionDimensionsOf(config));
  }

  @Test
  public void testSerdeWithNumericLookingPartitionDimension() throws Exception
  {
    // Dimension names are plain strings; a numeric-looking name is just a string.
    final KafkaIndexTaskTuningConfig config = roundTripWithStreamingPartitionsSpec("[\"123\"]");
    Assertions.assertEquals(List.of("123"), partitionDimensionsOf(config));
  }

  @Test
  public void testSerdeWithNullElementInPartitionDimensions() throws Exception
  {
    final KafkaIndexTaskTuningConfig config = roundTripWithStreamingPartitionsSpec("[\"tenant\", null]");
    Assertions.assertEquals(Arrays.asList("tenant", null), partitionDimensionsOf(config));
  }

  @Test
  public void testSerdeWithExplicitDimValueSetType() throws Exception
  {
    // An explicit "type": "dim_value_set" round-trips to the same spec as the untyped (default) form.
    final String jsonStr = "{\n"
                           + "  \"type\": \"kafka\",\n"
                           + "  \"streamingPartitionsSpec\": "
                           + "{\"type\": \"dim_value_set\", \"partitionDimensions\": [\"tenant\", \"region\"]}\n"
                           + "}";

    final KafkaIndexTaskTuningConfig config = (KafkaIndexTaskTuningConfig) mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(jsonStr, TuningConfig.class)),
        TuningConfig.class
    );

    Assertions.assertEquals(
        new DimensionValueSetPartitionsSpec(List.of("tenant", "region")),
        config.getStreamingPartitionsSpec()
    );
    Assertions.assertEquals(List.of("tenant", "region"), partitionDimensionsOf(config));
  }

  @Test
  public void testSerdeWithUnknownStreamingPartitionsSpecTypeIsRejected()
  {
    // An explicit but unknown type (e.g. a typo, or a subtype whose extension isn't loaded on this peon) must fail
    // rather than silently falling back to the default DimensionValueSetPartitionsSpec.
    final String jsonStr = "{\n"
                           + "  \"type\": \"kafka\",\n"
                           + "  \"streamingPartitionsSpec\": "
                           + "{\"type\": \"dim_value_sets\", \"partitionDimensions\": [\"tenant\"]}\n"
                           + "}";

    final Exception e = Assertions.assertThrows(
        Exception.class,
        () -> mapper.readValue(jsonStr, TuningConfig.class)
    );
    Assertions.assertTrue(
        e.getMessage().contains("dim_value_sets"),
        "Expected the unknown type id to be surfaced, got: " + e.getMessage()
    );
  }

  private KafkaIndexTaskTuningConfig roundTripWithStreamingPartitionsSpec(String partitionDimensionsJson)
      throws IOException
  {
    final String jsonStr = "{\n"
                           + "  \"type\": \"kafka\",\n"
                           + "  \"streamingPartitionsSpec\": {\"partitionDimensions\": " + partitionDimensionsJson + "}\n"
                           + "}";
    return (KafkaIndexTaskTuningConfig) mapper.readValue(
        mapper.writeValueAsString(mapper.readValue(jsonStr, TuningConfig.class)),
        TuningConfig.class
    );
  }

  private static List<String> partitionDimensionsOf(KafkaIndexTaskTuningConfig config)
  {
    return ((DimensionValueSetPartitionsSpec) config.getStreamingPartitionsSpec()).getPartitionDimensions();
  }

  @Test
  public void testConvert()
  {
    KafkaSupervisorTuningConfig original = new KafkaTuningConfigBuilder()
        .withIntermediatePersistPeriod(new Period("PT3S"))
        .withHandoffConditionTimeout(5L)
        .withNumPersistThreads(2)
        .withMaxRowsInMemory(1)
        .withMaxRowsPerSegment(2)
        .withMaxTotalRows(10L)
        .withMaxPendingPersists(4)
        .withIndexSpec(IndexSpec.getDefault())
        .withIndexSpecForIntermediatePersists(IndexSpec.getDefault())
        .withReportParseExceptions(true)
        .withMaxColumnsToMerge(5)
        .build();
    KafkaIndexTaskTuningConfig copy = original.convertToTaskTuningConfig();

    Assertions.assertEquals(original.getAppendableIndexSpec(), copy.getAppendableIndexSpec());
    Assertions.assertEquals(1, copy.getMaxRowsInMemory());
    Assertions.assertEquals(2, copy.getMaxRowsPerSegment().intValue());
    Assertions.assertNotEquals(null, copy.getMaxTotalRows());
    Assertions.assertEquals(10L, copy.getMaxTotalRows().longValue());
    Assertions.assertEquals(new Period("PT3S"), copy.getIntermediatePersistPeriod());
    Assertions.assertNull(copy.getBasePersistDirectory());
    Assertions.assertEquals(4, copy.getMaxPendingPersists());
    Assertions.assertEquals(IndexSpec.getDefault(), copy.getIndexSpec());
    Assertions.assertTrue(copy.isReportParseExceptions());
    Assertions.assertEquals(5L, copy.getHandoffConditionTimeout());
    Assertions.assertEquals(2, copy.getNumPersistThreads());
    Assertions.assertEquals(5, copy.getMaxColumnsToMerge());
  }

  @Test
  public void testSerdeWithModifiedTuningConfigAddedField() throws IOException
  {
    KafkaIndexTaskTuningConfig base = new KafkaIndexTaskTuningConfig(
        null,
        1,
        null,
        null,
        2,
        10L,
        new Period("PT3S"),
        new File("/tmp/xxx"),
        4,
        IndexSpec.getDefault(),
        IndexSpec.getDefault(),
        true,
        5L,
        null,
        null,
        null,
        true,
        42,
        42,
        2,
        -1,
        false,
        null
    );

    String serialized = mapper.writeValueAsString(base);
    TestModifiedKafkaIndexTaskTuningConfig deserialized =
        mapper.readValue(serialized, TestModifiedKafkaIndexTaskTuningConfig.class);

    Assertions.assertNull(deserialized.getExtra());
    Assertions.assertEquals(base.getAppendableIndexSpec(), deserialized.getAppendableIndexSpec());
    Assertions.assertEquals(base.getMaxRowsInMemory(), deserialized.getMaxRowsInMemory());
    Assertions.assertEquals(base.getMaxBytesInMemory(), deserialized.getMaxBytesInMemory());
    Assertions.assertEquals(base.getMaxRowsPerSegment(), deserialized.getMaxRowsPerSegment());
    Assertions.assertEquals(base.getMaxTotalRows(), deserialized.getMaxTotalRows());
    Assertions.assertEquals(base.getIntermediatePersistPeriod(), deserialized.getIntermediatePersistPeriod());
    Assertions.assertNull(deserialized.getBasePersistDirectory());
    Assertions.assertEquals(base.getMaxPendingPersists(), deserialized.getMaxPendingPersists());
    Assertions.assertEquals(base.getIndexSpec(), deserialized.getIndexSpec());
    Assertions.assertEquals(base.isReportParseExceptions(), deserialized.isReportParseExceptions());
    Assertions.assertEquals(base.getHandoffConditionTimeout(), deserialized.getHandoffConditionTimeout());
    Assertions.assertEquals(base.isResetOffsetAutomatically(), deserialized.isResetOffsetAutomatically());
    Assertions.assertEquals(base.getSegmentWriteOutMediumFactory(), deserialized.getSegmentWriteOutMediumFactory());
    Assertions.assertEquals(base.getIntermediateHandoffPeriod(), deserialized.getIntermediateHandoffPeriod());
    Assertions.assertEquals(base.isLogParseExceptions(), deserialized.isLogParseExceptions());
    Assertions.assertEquals(base.getMaxParseExceptions(), deserialized.getMaxParseExceptions());
    Assertions.assertEquals(base.getMaxSavedParseExceptions(), deserialized.getMaxSavedParseExceptions());
    Assertions.assertEquals(base.getNumPersistThreads(), deserialized.getNumPersistThreads());
    Assertions.assertEquals(base.getMaxColumnsToMerge(), deserialized.getMaxColumnsToMerge());
  }

  @Test
  public void testSerdeWithModifiedTuningConfigRemovedField() throws IOException
  {
    TestModifiedKafkaIndexTaskTuningConfig base = new TestModifiedKafkaIndexTaskTuningConfig(
        null,
        1,
        null,
        null,
        2,
        10L,
        new Period("PT3S"),
        4,
        IndexSpec.getDefault(),
        IndexSpec.getDefault(),
        true,
        5L,
        null,
        null,
        null,
        true,
        42,
        42,
        2,
        -1,
        "extra string"
    );

    String serialized = mapper.writeValueAsString(base);
    KafkaIndexTaskTuningConfig deserialized =
        mapper.readValue(serialized, KafkaIndexTaskTuningConfig.class);

    Assertions.assertEquals(base.getAppendableIndexSpec(), deserialized.getAppendableIndexSpec());
    Assertions.assertEquals(base.getMaxRowsInMemory(), deserialized.getMaxRowsInMemory());
    Assertions.assertEquals(base.getMaxBytesInMemory(), deserialized.getMaxBytesInMemory());
    Assertions.assertEquals(base.getMaxRowsPerSegment(), deserialized.getMaxRowsPerSegment());
    Assertions.assertEquals(base.getMaxTotalRows(), deserialized.getMaxTotalRows());
    Assertions.assertEquals(base.getIntermediatePersistPeriod(), deserialized.getIntermediatePersistPeriod());
    Assertions.assertEquals(base.getBasePersistDirectory(), deserialized.getBasePersistDirectory());
    Assertions.assertEquals(base.getMaxPendingPersists(), deserialized.getMaxPendingPersists());
    Assertions.assertEquals(base.getIndexSpec(), deserialized.getIndexSpec());
    Assertions.assertEquals(base.isReportParseExceptions(), deserialized.isReportParseExceptions());
    Assertions.assertEquals(base.getHandoffConditionTimeout(), deserialized.getHandoffConditionTimeout());
    Assertions.assertEquals(base.isResetOffsetAutomatically(), deserialized.isResetOffsetAutomatically());
    Assertions.assertEquals(base.getSegmentWriteOutMediumFactory(), deserialized.getSegmentWriteOutMediumFactory());
    Assertions.assertEquals(base.getIntermediateHandoffPeriod(), deserialized.getIntermediateHandoffPeriod());
    Assertions.assertEquals(base.isLogParseExceptions(), deserialized.isLogParseExceptions());
    Assertions.assertEquals(base.getMaxParseExceptions(), deserialized.getMaxParseExceptions());
    Assertions.assertEquals(base.getMaxSavedParseExceptions(), deserialized.getMaxSavedParseExceptions());
    Assertions.assertEquals(base.getNumPersistThreads(), deserialized.getNumPersistThreads());
    Assertions.assertEquals(base.getMaxColumnsToMerge(), deserialized.getMaxColumnsToMerge());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(KafkaIndexTaskTuningConfig.class)
                  .withPrefabValues(
                      IndexSpec.class,
                      IndexSpec.getDefault(),
                      IndexSpec.builder().withDimensionCompression(CompressionStrategy.ZSTD).build()
                  )
                  .usingGetClass()
                  .verify();
  }
}
