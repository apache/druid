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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.SegmentsSplitHintSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionFactory.LongEncodingStrategy;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class DataSourceCompactionConfigTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerdeBasic() throws IOException
  {
    final DataSourceCompactionConfig config = DataSourceCompactionConfig
        .builder()
        .forDataSource("dataSource")
        .withSkipOffsetFromLatest(new Period(3600))
        .withTaskContext(ImmutableMap.of("key", "val"))
        .build();
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(100_000_000_000_000L, fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getMaxRowsPerSegment(), fromJson.getMaxRowsPerSegment());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
    Assert.assertEquals(config.getGranularitySpec(), fromJson.getGranularitySpec());
    Assert.assertEquals(config.getEngine(), fromJson.getEngine());
  }

  @Test
  public void testSerdeWithMaxRowsPerSegment() throws IOException
  {
    final DataSourceCompactionConfig config = DataSourceCompactionConfig
        .builder()
        .forDataSource("dataSource")
        .withInputSegmentSizeBytes(500L)
        .withMaxRowsPerSegment(30)
        .withSkipOffsetFromLatest(new Period(3600))
        .withEngine(CompactionEngine.MSQ)
        .withTaskContext(ImmutableMap.of("key", "val"))
        .build();
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getMaxRowsPerSegment(), fromJson.getMaxRowsPerSegment());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
    Assert.assertEquals(config.getEngine(), fromJson.getEngine());
  }

  @Test
  public void testSerdeWithMaxTotalRows() throws IOException
  {
    final DataSourceCompactionConfig config = DataSourceCompactionConfig
        .builder()
        .forDataSource("dataSource")
        .withInputSegmentSizeBytes(500L)
        .withSkipOffsetFromLatest(new Period(3600))
        .withEngine(CompactionEngine.NATIVE)
        .withTaskContext(ImmutableMap.of("key", "val"))
        .build();
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getMaxRowsPerSegment(), fromJson.getMaxRowsPerSegment());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
    Assert.assertEquals(config.getEngine(), fromJson.getEngine());
  }

  @Test
  public void testSerdeMaxTotalRowsWithMaxRowsPerSegment() throws IOException
  {
    final DataSourceCompactionConfig config = DataSourceCompactionConfig
        .builder()
        .forDataSource("dataSource")
        .withInputSegmentSizeBytes(500L)
        .withMaxRowsPerSegment(10000)
        .withSkipOffsetFromLatest(new Period(3600))
        .withTaskContext(ImmutableMap.of("key", "val"))
        .build();
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getMaxRowsPerSegment(), fromJson.getMaxRowsPerSegment());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
  }

  @Test
  public void testSerdeUserCompactionTuningConfig() throws IOException
  {
    final UserCompactionTaskQueryTuningConfig tuningConfig = new UserCompactionTaskQueryTuningConfig(
        40000,
        null,
        2000L,
        null,
        new SegmentsSplitHintSpec(new HumanReadableBytes(100000L), null),
        new DynamicPartitionsSpec(1000, 20000L),
        IndexSpec.builder()
                 .withDimensionCompression(CompressionStrategy.LZ4)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(LongEncodingStrategy.LONGS)
                 .build(),
        IndexSpec.builder()
                 .withDimensionCompression(CompressionStrategy.LZ4)
                 .withMetricCompression(CompressionStrategy.UNCOMPRESSED)
                 .withLongEncoding(LongEncodingStrategy.AUTO)
                 .build(),
        2,
        1000L,
        TmpFileSegmentWriteOutMediumFactory.instance(),
        100,
        5,
        1000L,
        new Duration(3000L),
        7,
        1000,
        100,
        2
    );

    final String json = OBJECT_MAPPER.writeValueAsString(tuningConfig);
    final UserCompactionTaskQueryTuningConfig fromJson =
        OBJECT_MAPPER.readValue(json, UserCompactionTaskQueryTuningConfig.class);
    Assert.assertEquals(tuningConfig, fromJson);
  }

  @Test
  public void testSerdeUserCompactionTuningConfigWithAppendableIndexSpec() throws IOException
  {
    final UserCompactionTaskQueryTuningConfig tuningConfig = new UserCompactionTaskQueryTuningConfig(
        40000,
        new OnheapIncrementalIndex.Spec(true),
        2000L,
        null,
        new SegmentsSplitHintSpec(new HumanReadableBytes(100000L), null),
        new DynamicPartitionsSpec(1000, 20000L),
        IndexSpec.builder()
                 .withDimensionCompression(CompressionStrategy.LZ4)
                 .withMetricCompression(CompressionStrategy.LZ4)
                 .withLongEncoding(LongEncodingStrategy.LONGS)
                 .build(),
        IndexSpec.builder()
                 .withDimensionCompression(CompressionStrategy.LZ4)
                 .withMetricCompression(CompressionStrategy.UNCOMPRESSED)
                 .withLongEncoding(LongEncodingStrategy.AUTO)
                 .build(),
        2,
        1000L,
        TmpFileSegmentWriteOutMediumFactory.instance(),
        100,
        5,
        1000L,
        new Duration(3000L),
        7,
        1000,
        100,
        2
    );

    final String json = OBJECT_MAPPER.writeValueAsString(tuningConfig);
    final UserCompactionTaskQueryTuningConfig fromJson =
        OBJECT_MAPPER.readValue(json, UserCompactionTaskQueryTuningConfig.class);
    Assert.assertEquals(tuningConfig, fromJson);
  }

  @Test
  public void testSerdeGranularitySpec() throws IOException
  {
    final DataSourceCompactionConfig config = DataSourceCompactionConfig
        .builder()
        .forDataSource("dataSource")
        .withInputSegmentSizeBytes(500L)
        .withSkipOffsetFromLatest(new Period(3600))
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null))
        .withTaskContext(ImmutableMap.of("key", "val"))
        .build();
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getMaxRowsPerSegment(), fromJson.getMaxRowsPerSegment());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
    Assert.assertEquals(config.getGranularitySpec(), fromJson.getGranularitySpec());
  }

  @Test
  public void testSerdeGranularitySpecWithQueryGranularity() throws Exception
  {
    final DataSourceCompactionConfig config = DataSourceCompactionConfig
        .builder()
        .forDataSource("dataSource")
        .withInputSegmentSizeBytes(500L)
        .withSkipOffsetFromLatest(new Period(3600))
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(null, Granularities.YEAR, null))
        .withTaskContext(ImmutableMap.of("key", "val"))
        .build();
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getMaxRowsPerSegment(), fromJson.getMaxRowsPerSegment());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
    Assert.assertEquals(config.getGranularitySpec(), fromJson.getGranularitySpec());
    Assert.assertNotNull(config.getGranularitySpec());
    Assert.assertNotNull(fromJson.getGranularitySpec());
    Assert.assertEquals(config.getGranularitySpec().getQueryGranularity(), fromJson.getGranularitySpec().getQueryGranularity());
  }

  @Test
  public void testSerdeWithNullGranularitySpec() throws IOException
  {
    final DataSourceCompactionConfig config = DataSourceCompactionConfig
        .builder()
        .forDataSource("dataSource")
        .withInputSegmentSizeBytes(500L)
        .withSkipOffsetFromLatest(new Period(3600))
        .withTaskContext(ImmutableMap.of("key", "val"))
        .build();
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getMaxRowsPerSegment(), fromJson.getMaxRowsPerSegment());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
    Assert.assertEquals(config.getGranularitySpec(), fromJson.getGranularitySpec());
  }

  @Test
  public void testSerdeGranularitySpecWithNullValues() throws IOException
  {
    final DataSourceCompactionConfig config = DataSourceCompactionConfig
        .builder()
        .forDataSource("dataSource")
        .withInputSegmentSizeBytes(500L)
        .withSkipOffsetFromLatest(new Period(3600))
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(null, null, null))
        .withTaskContext(ImmutableMap.of("key", "val"))
        .build();
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getMaxRowsPerSegment(), fromJson.getMaxRowsPerSegment());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
    Assert.assertEquals(config.getGranularitySpec(), fromJson.getGranularitySpec());
  }

  @Test
  public void testSerdeGranularitySpecWithRollup() throws IOException
  {
    final DataSourceCompactionConfig config = DataSourceCompactionConfig
        .builder()
        .forDataSource("dataSource")
        .withInputSegmentSizeBytes(500L)
        .withSkipOffsetFromLatest(new Period(3600))
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(null, null, true))
        .withTaskContext(ImmutableMap.of("key", "val"))
        .build();
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getMaxRowsPerSegment(), fromJson.getMaxRowsPerSegment());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
    Assert.assertEquals(config.getGranularitySpec(), fromJson.getGranularitySpec());
    Assert.assertNotNull(config.getGranularitySpec());
    Assert.assertNotNull(fromJson.getGranularitySpec());
    Assert.assertEquals(config.getGranularitySpec().isRollup(), fromJson.getGranularitySpec().isRollup());
  }

  @Test
  public void testSerdeIOConfigWithNonNullDropExisting() throws IOException
  {
    final DataSourceCompactionConfig config = DataSourceCompactionConfig
        .builder()
        .forDataSource("dataSource")
        .withInputSegmentSizeBytes(500L)
        .withSkipOffsetFromLatest(new Period(3600))
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null))
        .withIoConfig(new UserCompactionTaskIOConfig(true))
        .withTaskContext(ImmutableMap.of("key", "val"))
        .build();
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getMaxRowsPerSegment(), fromJson.getMaxRowsPerSegment());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
    Assert.assertEquals(config.getGranularitySpec(), fromJson.getGranularitySpec());
    Assert.assertEquals(config.getIoConfig(), fromJson.getIoConfig());
  }

  @Test
  public void testSerdeIOConfigWithNullDropExisting() throws IOException
  {
    final DataSourceCompactionConfig config = DataSourceCompactionConfig
        .builder()
        .forDataSource("dataSource")
        .withInputSegmentSizeBytes(500L)
        .withSkipOffsetFromLatest(new Period(3600))
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null))
        .withIoConfig(new UserCompactionTaskIOConfig(null))
        .withTaskContext(ImmutableMap.of("key", "val"))
        .build();
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getMaxRowsPerSegment(), fromJson.getMaxRowsPerSegment());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
    Assert.assertEquals(config.getGranularitySpec(), fromJson.getGranularitySpec());
    Assert.assertEquals(config.getIoConfig(), fromJson.getIoConfig());
  }

  @Test
  public void testSerdeDimensionsSpec() throws IOException
  {
    final DataSourceCompactionConfig config = DataSourceCompactionConfig
        .builder()
        .forDataSource("dataSource")
        .withInputSegmentSizeBytes(500L)
        .withSkipOffsetFromLatest(new Period(3600))
        .withDimensionsSpec(
            new UserCompactionTaskDimensionsConfig(
                DimensionsSpec.getDefaultSchemas(ImmutableList.of("foo"))
            )
        )
        .withTaskContext(ImmutableMap.of("key", "val"))
        .build();
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getMaxRowsPerSegment(), fromJson.getMaxRowsPerSegment());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
    Assert.assertEquals(config.getDimensionsSpec(), fromJson.getDimensionsSpec());
  }

  @Test
  public void testSerdeTransformSpec() throws IOException
  {
    NullHandling.initializeForTests();
    final DataSourceCompactionConfig config = DataSourceCompactionConfig
        .builder()
        .forDataSource("dataSource")
        .withInputSegmentSizeBytes(500L)
        .withSkipOffsetFromLatest(new Period(3600))
        .withTransformSpec(new UserCompactionTaskTransformConfig(new SelectorDimFilter("dim1", "foo", null)))
        .withTaskContext(ImmutableMap.of("key", "val"))
        .build();
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getMaxRowsPerSegment(), fromJson.getMaxRowsPerSegment());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
    Assert.assertEquals(config.getTransformSpec(), fromJson.getTransformSpec());
  }

  @Test
  public void testSerdeMetricsSpec() throws IOException
  {
    final DataSourceCompactionConfig config = DataSourceCompactionConfig
        .builder()
        .forDataSource("dataSource")
        .withInputSegmentSizeBytes(500L)
        .withSkipOffsetFromLatest(new Period(3600))
        .withMetricsSpec(new AggregatorFactory[]{new CountAggregatorFactory("cnt")})
        .withTaskContext(ImmutableMap.of("key", "val"))
        .build();
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getMaxRowsPerSegment(), fromJson.getMaxRowsPerSegment());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
    Assert.assertEquals(config.getMetricsSpec(), fromJson.getMetricsSpec());
  }
}
