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

package org.apache.druid.server.compaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class CompactionStatusTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  private static final DataSegment WIKI_SEGMENT
      = DataSegment.builder()
                   .dataSource(TestDataSource.WIKI)
                   .interval(Intervals.of("2013-01-01/PT1H"))
                   .size(100_000_000L)
                   .version("v1")
                   .build();

  @Test
  public void testFindPartitionsSpecWhenGivenIsNull()
  {
    final ClientCompactionTaskQueryTuningConfig tuningConfig
        = ClientCompactionTaskQueryTuningConfig.from(null);
    Assert.assertEquals(
        new DynamicPartitionsSpec(null, Long.MAX_VALUE),
        CompactionStatus.findPartitionsSpecFromConfig(tuningConfig)
    );
  }

  @Test
  public void testFindPartitionsSpecWhenGivenIsDynamicWithNullMaxTotalRows()
  {
    final PartitionsSpec partitionsSpec = new DynamicPartitionsSpec(null, null);
    final ClientCompactionTaskQueryTuningConfig tuningConfig
        = ClientCompactionTaskQueryTuningConfig.from(createCompactionConfig(partitionsSpec));
    Assert.assertEquals(
        new DynamicPartitionsSpec(null, Long.MAX_VALUE),
        CompactionStatus.findPartitionsSpecFromConfig(tuningConfig)
    );
  }

  @Test
  public void testFindPartitionsSpecWhenGivenIsDynamicWithMaxTotalRows()
  {
    final PartitionsSpec partitionsSpec = new DynamicPartitionsSpec(null, 1000L);
    final ClientCompactionTaskQueryTuningConfig tuningConfig
        = ClientCompactionTaskQueryTuningConfig.from(createCompactionConfig(partitionsSpec));
    Assert.assertEquals(
        partitionsSpec,
        CompactionStatus.findPartitionsSpecFromConfig(tuningConfig)
    );
  }

  @Test
  public void testFindPartitionsSpecWhenGivenIsDynamicWithMaxRowsPerSegment()
  {
    final PartitionsSpec partitionsSpec = new DynamicPartitionsSpec(100, 1000L);
    final ClientCompactionTaskQueryTuningConfig tuningConfig
        = ClientCompactionTaskQueryTuningConfig.from(createCompactionConfig(partitionsSpec));
    Assert.assertEquals(
        partitionsSpec,
        CompactionStatus.findPartitionsSpecFromConfig(tuningConfig)
    );
  }

  @Test
  public void testFindPartitionsSpecFromConfigWithDeprecatedMaxRowsPerSegmentAndMaxTotalRowsReturnGivenValues()
  {
    final DataSourceCompactionConfig config = new DataSourceCompactionConfig(
        "datasource",
        null,
        null,
        100,
        null,
        new UserCompactionTaskQueryTuningConfig(
            null,
            null,
            null,
            1000L,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        ),
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
    Assert.assertEquals(
        new DynamicPartitionsSpec(100, 1000L),
        CompactionStatus.findPartitionsSpecFromConfig(
            ClientCompactionTaskQueryTuningConfig.from(config)
        )
    );
  }

  @Test
  public void testFindPartitionsSpecWhenGivenIsHashed()
  {
    final PartitionsSpec partitionsSpec =
        new HashedPartitionsSpec(null, 100, Collections.singletonList("dim"));
    final ClientCompactionTaskQueryTuningConfig tuningConfig
        = ClientCompactionTaskQueryTuningConfig.from(createCompactionConfig(partitionsSpec));
    Assert.assertEquals(
        partitionsSpec,
        CompactionStatus.findPartitionsSpecFromConfig(tuningConfig)
    );
  }

  @Test
  public void testFindPartitionsSpecWhenGivenIsRangeWithMaxRows()
  {
    final PartitionsSpec partitionsSpec =
        new DimensionRangePartitionsSpec(null, 10000, Collections.singletonList("dim"), false);
    final ClientCompactionTaskQueryTuningConfig tuningConfig
        = ClientCompactionTaskQueryTuningConfig.from(createCompactionConfig(partitionsSpec));
    Assert.assertEquals(
        partitionsSpec,
        CompactionStatus.findPartitionsSpecFromConfig(tuningConfig)
    );
  }

  @Test
  public void testFindPartitionsSpecWhenGivenIsRangeWithTargetRows()
  {
    final PartitionsSpec partitionsSpec =
        new DimensionRangePartitionsSpec(10000, null, Collections.singletonList("dim"), false);
    final ClientCompactionTaskQueryTuningConfig tuningConfig
        = ClientCompactionTaskQueryTuningConfig.from(createCompactionConfig(partitionsSpec));
    Assert.assertEquals(
        new DimensionRangePartitionsSpec(null, 15000, Collections.singletonList("dim"), false),
        CompactionStatus.findPartitionsSpecFromConfig(tuningConfig)
    );
  }

  @Test
  public void testStatusWhenLastCompactionStateIsNull()
  {
    verifyCompactionStatusIsPendingBecause(
        null,
        DataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        "not compacted yet"
    );
  }

  @Test
  public void testStatusWhenLastCompactionStateIsEmpty()
  {
    verifyCompactionStatusIsPendingBecause(
        new CompactionState(null, null, null, null, null, null),
        DataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        "'partitionsSpec' mismatch: required['dynamic' with 5,000,000 rows], current[null]"
    );
  }

  @Test
  public void testStatusOnPartitionsSpecMismatch()
  {
    final PartitionsSpec currentPartitionsSpec = new DynamicPartitionsSpec(100, 0L);

    final CompactionState lastCompactionState
        = new CompactionState(currentPartitionsSpec, null, null, null, null, null);
    final DataSourceCompactionConfig compactionConfig
        = DataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build();

    verifyCompactionStatusIsPendingBecause(
        lastCompactionState,
        compactionConfig,
        "'partitionsSpec' mismatch: required['dynamic' with 5,000,000 rows],"
        + " current['dynamic' with 100 rows]"
    );
  }

  @Test
  public void testStatusOnIndexSpecMismatch()
  {
    final IndexSpec currentIndexSpec
        = IndexSpec.builder().withDimensionCompression(CompressionStrategy.ZSTD).build();

    final PartitionsSpec currentPartitionsSpec = new DynamicPartitionsSpec(100, 0L);
    final CompactionState lastCompactionState = new CompactionState(
        currentPartitionsSpec,
        null,
        null,
        null,
        currentIndexSpec.asMap(OBJECT_MAPPER),
        null
    );
    final DataSourceCompactionConfig compactionConfig = DataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withTuningConfig(createTuningConfig(currentPartitionsSpec, null))
        .build();

    verifyCompactionStatusIsPendingBecause(
        lastCompactionState,
        compactionConfig,
        "'indexSpec' mismatch: "
        + "required[IndexSpec{bitmapSerdeFactory=RoaringBitmapSerdeFactory{},"
        + " dimensionCompression=lz4, stringDictionaryEncoding=Utf8{},"
        + " metricCompression=lz4, longEncoding=longs, complexMetricCompression=null,"
        + " jsonCompression=null, segmentLoader=null}], "
        + "current[IndexSpec{bitmapSerdeFactory=RoaringBitmapSerdeFactory{},"
        + " dimensionCompression=zstd, stringDictionaryEncoding=Utf8{},"
        + " metricCompression=lz4, longEncoding=longs, complexMetricCompression=null,"
        + " jsonCompression=null, segmentLoader=null}]"
    );
  }

  @Test
  public void testStatusOnSegmentGranularityMismatch()
  {
    final GranularitySpec currentGranularitySpec
        = new UniformGranularitySpec(Granularities.HOUR, null, null);

    final PartitionsSpec currentPartitionsSpec = new DynamicPartitionsSpec(100, 0L);
    final IndexSpec currentIndexSpec
        = IndexSpec.builder().withDimensionCompression(CompressionStrategy.ZSTD).build();
    final CompactionState lastCompactionState = new CompactionState(
        currentPartitionsSpec,
        null,
        null,
        null,
        currentIndexSpec.asMap(OBJECT_MAPPER),
        currentGranularitySpec.asMap(OBJECT_MAPPER)
    );
    final DataSourceCompactionConfig compactionConfig = DataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withTuningConfig(createTuningConfig(currentPartitionsSpec, currentIndexSpec))
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null))
        .build();

    verifyCompactionStatusIsPendingBecause(
        lastCompactionState,
        compactionConfig,
        "'segmentGranularity' mismatch: required[DAY], current[HOUR]"
    );
  }

  @Test
  public void testStatusWhenLastCompactionStateSameAsRequired()
  {
    final GranularitySpec currentGranularitySpec
        = new UniformGranularitySpec(Granularities.HOUR, null, null);
    final PartitionsSpec currentPartitionsSpec = new DynamicPartitionsSpec(100, 0L);
    final IndexSpec currentIndexSpec
        = IndexSpec.builder().withDimensionCompression(CompressionStrategy.ZSTD).build();
    final CompactionState lastCompactionState = new CompactionState(
        currentPartitionsSpec,
        null,
        null,
        null,
        currentIndexSpec.asMap(OBJECT_MAPPER),
        currentGranularitySpec.asMap(OBJECT_MAPPER)
    );
    final DataSourceCompactionConfig compactionConfig = DataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withTuningConfig(createTuningConfig(currentPartitionsSpec, currentIndexSpec))
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null))
        .build();

    final DataSegment segment = DataSegment.builder(WIKI_SEGMENT).lastCompactionState(lastCompactionState).build();
    final CompactionStatus status = CompactionStatus.compute(
        CompactionCandidate.from(Collections.singletonList(segment)),
        compactionConfig,
        OBJECT_MAPPER
    );
    Assert.assertTrue(status.isComplete());
  }

  private void verifyCompactionStatusIsPendingBecause(
      CompactionState lastCompactionState,
      DataSourceCompactionConfig compactionConfig,
      String expectedReason
  )
  {
    final DataSegment segment
        = DataSegment.builder(WIKI_SEGMENT)
                     .lastCompactionState(lastCompactionState)
                     .build();
    final CompactionStatus status = CompactionStatus.compute(
        CompactionCandidate.from(Collections.singletonList(segment)),
        compactionConfig,
        OBJECT_MAPPER
    );

    Assert.assertFalse(status.isComplete());
    Assert.assertEquals(expectedReason, status.getReason());
  }

  private static DataSourceCompactionConfig createCompactionConfig(
      PartitionsSpec partitionsSpec
  )
  {
    return DataSourceCompactionConfig.builder()
                                     .forDataSource(TestDataSource.WIKI)
                                     .withTuningConfig(createTuningConfig(partitionsSpec, null))
                                     .build();
  }

  private static UserCompactionTaskQueryTuningConfig createTuningConfig(
      PartitionsSpec partitionsSpec,
      IndexSpec indexSpec
  )
  {
    return new UserCompactionTaskQueryTuningConfig(
        null,
        null, null, null, null, partitionsSpec, indexSpec, null, null,
        null, null, null, null, null, null, null, null, null, null
    );
  }
}
