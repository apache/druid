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

import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.indexer.granularity.GranularitySpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.metadata.DefaultIndexingStateFingerprintMapper;
import org.apache.druid.segment.metadata.HeapMemoryIndexingStateStorage;
import org.apache.druid.segment.metadata.IndexingStateCache;
import org.apache.druid.segment.metadata.IndexingStateFingerprintMapper;
import org.apache.druid.segment.nested.NestedCommonFormatColumnFormatSpec;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class CompactionStatusTest
{
  private static final DataSegment WIKI_SEGMENT
      = DataSegment.builder(SegmentId.of(TestDataSource.WIKI, Intervals.of("2013-01-01/PT1H"), "v1", 0))
                   .size(100_000_000L)
                   .build();
  private static final DataSegment WIKI_SEGMENT_2
      = DataSegment.builder(SegmentId.of(TestDataSource.WIKI, Intervals.of("2013-01-01/PT1H"), "v1", 1))
                   .size(100_000_000L)
                   .build();

  private HeapMemoryIndexingStateStorage indexingStateStorage;
  private IndexingStateCache indexingStateCache;
  private IndexingStateFingerprintMapper fingerprintMapper;

  @Before
  public void setUp()
  {
    indexingStateStorage = new HeapMemoryIndexingStateStorage();
    indexingStateCache = new IndexingStateCache();
    fingerprintMapper = new DefaultIndexingStateFingerprintMapper(
        indexingStateCache,
        new DefaultObjectMapper()
    );
  }

  /**
   * Helper to sync the cache with states stored in the manager (for tests that persist states).
   */
  private void syncCacheFromManager()
  {
    indexingStateCache.resetIndexingStatesForPublishedSegments(indexingStateStorage.getAllStoredStates());
  }

  @Test
  public void testFindPartitionsSpecWhenGivenIsNull()
  {
    final ClientCompactionTaskQueryTuningConfig tuningConfig
        = ClientCompactionTaskQueryTuningConfig.from(null);
    Assert.assertNull(
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
    final DataSourceCompactionConfig config =
        InlineSchemaDataSourceCompactionConfig.builder()
                                              .forDataSource("datasource")
                                              .withMaxRowsPerSegment(100)
                                              .withTuningConfig(
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
                                            )
                                        )
                                              .build();
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
        InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        "not compacted yet"
    );
  }

  @Test
  public void testStatusWhenLastCompactionStateIsEmpty()
  {
    final PartitionsSpec requiredPartitionsSpec = new DynamicPartitionsSpec(5_000_000, null);
    verifyCompactionStatusIsPendingBecause(
        new CompactionState(null, null, null, null, null, null, null),
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .withTuningConfig(createTuningConfig(requiredPartitionsSpec, null))
            .forDataSource(TestDataSource.WIKI)
            .build(),
        "'partitionsSpec' mismatch: required['dynamic' with 5,000,000 rows], current[null]"
    );
  }

  @Test
  public void testStatusOnPartitionsSpecMismatch()
  {
    final PartitionsSpec requiredPartitionsSpec = new DynamicPartitionsSpec(5_000_000, null);
    final PartitionsSpec currentPartitionsSpec = new DynamicPartitionsSpec(100, null);

    final CompactionState lastCompactionState
        = new CompactionState(currentPartitionsSpec, null, null, null, null, null, null);
    final DataSourceCompactionConfig compactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .withTuningConfig(createTuningConfig(requiredPartitionsSpec, null))
        .forDataSource(TestDataSource.WIKI)
        .build();

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

    final PartitionsSpec currentPartitionsSpec = new DynamicPartitionsSpec(100, null);
    final CompactionState lastCompactionState = new CompactionState(
        currentPartitionsSpec,
        null,
        null,
        null,
        currentIndexSpec,
        null,
        null
    );
    final DataSourceCompactionConfig compactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withTuningConfig(createTuningConfig(currentPartitionsSpec, null))
        .build();

    verifyCompactionStatusIsPendingBecause(
        lastCompactionState,
        compactionConfig,
        "'indexSpec' mismatch: "
        + "required[IndexSpec{bitmapSerdeFactory=RoaringBitmapSerdeFactory{},"
        + " metadataCompression=none,"
        + " dimensionCompression=lz4, stringDictionaryEncoding=Utf8{},"
        + " metricCompression=lz4, longEncoding=longs, complexMetricCompression=null,"
        + " autoColumnFormatSpec=null, jsonCompression=null, segmentLoader=null}], "
        + "current[IndexSpec{bitmapSerdeFactory=RoaringBitmapSerdeFactory{},"
        + " metadataCompression=none,"
        + " dimensionCompression=zstd, stringDictionaryEncoding=Utf8{},"
        + " metricCompression=lz4, longEncoding=longs, complexMetricCompression=null,"
        + " autoColumnFormatSpec=null, jsonCompression=null, segmentLoader=null}]"
    );
  }

  @Test
  public void testStatusOnSegmentGranularityMismatch()
  {
    final GranularitySpec currentGranularitySpec
        = new UniformGranularitySpec(Granularities.HOUR, null, null);

    final PartitionsSpec currentPartitionsSpec = new DynamicPartitionsSpec(100, null);
    final IndexSpec currentIndexSpec
        = IndexSpec.builder().withDimensionCompression(CompressionStrategy.ZSTD).build();
    final CompactionState lastCompactionState = new CompactionState(
        currentPartitionsSpec,
        null,
        null,
        null,
        currentIndexSpec,
        currentGranularitySpec,
        null
    );
    final DataSourceCompactionConfig compactionConfig = InlineSchemaDataSourceCompactionConfig
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
    final PartitionsSpec currentPartitionsSpec = new DynamicPartitionsSpec(100, null);
    final IndexSpec currentIndexSpec
        = IndexSpec.builder().withDimensionCompression(CompressionStrategy.ZSTD).build();
    final CompactionState lastCompactionState = new CompactionState(
        currentPartitionsSpec,
        null,
        null,
        null,
        currentIndexSpec,
        currentGranularitySpec,
        null
    );
    final DataSourceCompactionConfig compactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withTuningConfig(createTuningConfig(currentPartitionsSpec, currentIndexSpec))
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null))
        .build();

    final DataSegment segment = DataSegment.builder(WIKI_SEGMENT).lastCompactionState(lastCompactionState).build();
    final CompactionStatus status = CompactionStatus.compute(
        CompactionCandidate.from(List.of(segment), Granularities.HOUR),
        compactionConfig,
        fingerprintMapper
    );
    Assert.assertTrue(status.isComplete());
  }

  @Test
  public void testStatusWhenProjectionsMatch()
  {
    final GranularitySpec currentGranularitySpec
        = new UniformGranularitySpec(Granularities.HOUR, null, null);
    final PartitionsSpec currentPartitionsSpec = new DynamicPartitionsSpec(100, null);
    final IndexSpec currentIndexSpec
        = IndexSpec.builder().withDimensionCompression(CompressionStrategy.ZSTD).build();
    final AggregateProjectionSpec projection1 =
        AggregateProjectionSpec.builder("foo")
                               .virtualColumns(
                                   Granularities.toVirtualColumn(
                                       Granularities.HOUR,
                                       Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
                                   )
                               )
                               .groupingColumns(
                                   new LongDimensionSchema(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME),
                                   new StringDimensionSchema("a")
                               )
                               .aggregators(
                                   new LongSumAggregatorFactory("sum_long", "long")
                               )
                               .build();
    final CompactionState lastCompactionState = new CompactionState(
        currentPartitionsSpec,
        null,
        null,
        null,
        currentIndexSpec,
        currentGranularitySpec,
        List.of(projection1)
    );
    final DataSourceCompactionConfig compactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withTuningConfig(createTuningConfig(currentPartitionsSpec, currentIndexSpec))
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null))
        .withProjections(List.of(projection1))
        .build();

    final DataSegment segment = DataSegment.builder(WIKI_SEGMENT).lastCompactionState(lastCompactionState).build();
    final CompactionStatus status = CompactionStatus.compute(
        CompactionCandidate.from(List.of(segment), Granularities.HOUR),
        compactionConfig,
        fingerprintMapper
    );
    Assert.assertTrue(status.isComplete());
  }

  @Test
  public void testStatusWhenProjectionsMismatch()
  {
    final GranularitySpec currentGranularitySpec
        = new UniformGranularitySpec(Granularities.HOUR, null, null);
    final PartitionsSpec currentPartitionsSpec = new DynamicPartitionsSpec(100, null);
    final IndexSpec currentIndexSpec
        = IndexSpec.builder().withDimensionCompression(CompressionStrategy.ZSTD).build();
    final AggregateProjectionSpec projection1 =
        AggregateProjectionSpec.builder("1")
                               .virtualColumns(
                                   Granularities.toVirtualColumn(
                                       Granularities.HOUR,
                                       Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
                                   )
                               )
                               .groupingColumns(
                                   new LongDimensionSchema(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME),
                                   new StringDimensionSchema("a")
                               )
                               .aggregators(
                                   new LongSumAggregatorFactory("sum_long", "long")
                               )
                               .build();
    final AggregateProjectionSpec projection2 =
        AggregateProjectionSpec.builder("2")
                               .aggregators(new LongSumAggregatorFactory("sum_long", "long"))
                               .build();

    final CompactionState lastCompactionState = new CompactionState(
        currentPartitionsSpec,
        null,
        null,
        null,
        currentIndexSpec,
        currentGranularitySpec,
        List.of(projection1)
    );
    final DataSourceCompactionConfig compactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withTuningConfig(createTuningConfig(currentPartitionsSpec, currentIndexSpec))
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null))
        .withProjections(List.of(projection1, projection2))
        .build();

    final DataSegment segment = DataSegment.builder(WIKI_SEGMENT).lastCompactionState(lastCompactionState).build();
    final CompactionStatus status = CompactionStatus.compute(
        CompactionCandidate.from(List.of(segment), Granularities.HOUR),
        compactionConfig,
        fingerprintMapper
    );
    Assert.assertFalse(status.isComplete());
  }

  @Test
  public void testStatusWhenAutoSchemaMatch()
  {
    final GranularitySpec currentGranularitySpec
        = new UniformGranularitySpec(Granularities.HOUR, null, null);
    final PartitionsSpec currentPartitionsSpec = new DynamicPartitionsSpec(100, null);

    final CompactionState lastCompactionState = new CompactionState(
        currentPartitionsSpec,
        DimensionsSpec.builder()
                      .setDimensions(
                          List.of(
                              AutoTypeColumnSchema.of("x").getEffectiveSchema(IndexSpec.getDefault().getEffectiveSpec()),
                              AutoTypeColumnSchema.of("y").getEffectiveSchema(IndexSpec.getDefault().getEffectiveSpec())
                          )
                      )
                      .build(),
        null,
        null,
        IndexSpec.getDefault().getEffectiveSpec(),
        currentGranularitySpec,
        Collections.emptyList()
    );
    final DataSourceCompactionConfig compactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withDimensionsSpec(
            new UserCompactionTaskDimensionsConfig(
                List.of(
                    new AutoTypeColumnSchema(
                        "x",
                        null,
                        NestedCommonFormatColumnFormatSpec.builder()
                                                          .setDoubleColumnCompression(CompressionStrategy.LZ4)
                                                          .build()
                    ),
                    AutoTypeColumnSchema.of("y")
                )
            )
        )
        .withTuningConfig(createTuningConfig(currentPartitionsSpec, IndexSpec.getDefault()))
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null))
        .withProjections(Collections.emptyList())
        .build();

    final DataSegment segment = DataSegment.builder(WIKI_SEGMENT).lastCompactionState(lastCompactionState).build();
    final CompactionStatus status = CompactionStatus.compute(
        CompactionCandidate.from(List.of(segment), null),
        compactionConfig,
        fingerprintMapper
    );
    Assert.assertTrue(status.isComplete());
  }

  @Test
  public void testStatusWhenAutoSchemaMismatch()
  {
    final GranularitySpec currentGranularitySpec
        = new UniformGranularitySpec(Granularities.HOUR, null, null);
    final PartitionsSpec currentPartitionsSpec = new DynamicPartitionsSpec(100, null);

    final CompactionState lastCompactionState = new CompactionState(
        currentPartitionsSpec,
        DimensionsSpec.builder()
                      .setDimensions(
                          List.of(
                              AutoTypeColumnSchema.of("x").getEffectiveSchema(IndexSpec.getDefault()),
                              AutoTypeColumnSchema.of("y").getEffectiveSchema(IndexSpec.getDefault())
                          )
                      )
                      .build(),
        null,
        null,
        IndexSpec.getDefault(),
        currentGranularitySpec,
        Collections.emptyList()
    );
    final DataSourceCompactionConfig compactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withDimensionsSpec(
            new UserCompactionTaskDimensionsConfig(
                List.of(
                    new AutoTypeColumnSchema(
                        "x",
                        null,
                        NestedCommonFormatColumnFormatSpec.builder()
                                                          .setDoubleColumnCompression(CompressionStrategy.ZSTD)
                                                          .build()
                    ),
                    AutoTypeColumnSchema.of("y")
                )
            )
        )
        .withTuningConfig(createTuningConfig(currentPartitionsSpec, IndexSpec.getDefault()))
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null))
        .withProjections(Collections.emptyList())
        .build();

    final DataSegment segment = DataSegment.builder(WIKI_SEGMENT).lastCompactionState(lastCompactionState).build();
    final CompactionStatus status = CompactionStatus.compute(
        CompactionCandidate.from(List.of(segment), null),
        compactionConfig,
        fingerprintMapper
    );
    Assert.assertFalse(status.isComplete());
  }

  @Test
  public void test_evaluate_needsCompactionWhenAllSegmentsHaveUnexpectedIndexingStateFingerprint()
  {
    List<DataSegment> segments = List.of(
        DataSegment.builder(WIKI_SEGMENT).indexingStateFingerprint("wrongFingerprint").build(),
        DataSegment.builder(WIKI_SEGMENT_2).indexingStateFingerprint("wrongFingerprint").build()
    );

    final DataSourceCompactionConfig oldCompactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null))
        .build();
    CompactionState wrongState = oldCompactionConfig.toCompactionState();

    final DataSourceCompactionConfig compactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null))
        .build();

    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "wrongFingerprint", wrongState, DateTimes.nowUtc());
    syncCacheFromManager();

    verifyEvaluationNeedsCompactionBecauseWithCustomSegments(
        CompactionCandidate.from(segments, null),
        compactionConfig,
        "'segmentGranularity' mismatch: required[DAY], current[HOUR]"
    );
  }

  @Test
  public void test_evaluate_needsCompactionWhenSomeSegmentsHaveUnexpectedIndexingStateFingerprint()
  {
    final DataSourceCompactionConfig oldCompactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null))
        .build();
    CompactionState wrongState = oldCompactionConfig.toCompactionState();

    final DataSourceCompactionConfig compactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null))
        .build();

    CompactionState expectedState = compactionConfig.toCompactionState();

    String expectedFingerprint = fingerprintMapper.generateFingerprint(TestDataSource.WIKI, expectedState);

    List<DataSegment> segments = List.of(
        DataSegment.builder(WIKI_SEGMENT).indexingStateFingerprint(expectedFingerprint).build(),
        DataSegment.builder(WIKI_SEGMENT_2).indexingStateFingerprint("wrongFingerprint").build()
    );

    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, expectedFingerprint, expectedState, DateTimes.nowUtc());
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "wrongFingerprint", wrongState, DateTimes.nowUtc());
    syncCacheFromManager();

    verifyEvaluationNeedsCompactionBecauseWithCustomSegments(
        CompactionCandidate.from(segments, null),
        compactionConfig,
        "'segmentGranularity' mismatch: required[DAY], current[HOUR]"
    );
  }

  @Test
  public void test_evaluate_noCompacationIfUnexpectedFingerprintHasExpectedIndexingState()
  {
    List<DataSegment> segments = List.of(
        DataSegment.builder(WIKI_SEGMENT).indexingStateFingerprint("wrongFingerprint").build()
    );
    final DataSourceCompactionConfig compactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null))
        .build();

    CompactionState expectedState = compactionConfig.toCompactionState();
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "wrongFingerprint", expectedState, DateTimes.nowUtc());
    syncCacheFromManager();

    final CompactionStatus status = CompactionStatus.compute(
        CompactionCandidate.from(segments, null),
        compactionConfig,
        fingerprintMapper
    );
    Assert.assertTrue(status.isComplete());
  }

  @Test
  public void test_evaluate_needsCompactionWhenUnexpectedFingerprintAndNoFingerprintInMetadataStore()
  {
    List<DataSegment> segments = List.of(
        DataSegment.builder(WIKI_SEGMENT).indexingStateFingerprint("wrongFingerprint").build()
    );
    final DataSourceCompactionConfig compactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null))
        .build();

    verifyEvaluationNeedsCompactionBecauseWithCustomSegments(
        CompactionCandidate.from(segments, null),
        compactionConfig,
        "One or more fingerprinted segments do not have a cached indexing state"
    );
  }

  @Test
  public void test_evaluate_noCompactionWhenAllSegmentsHaveExpectedIndexingStateFingerprint()
  {
    final DataSourceCompactionConfig compactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null))
        .build();

    CompactionState expectedState = compactionConfig.toCompactionState();

    String expectedFingerprint = fingerprintMapper.generateFingerprint(TestDataSource.WIKI, expectedState);

    List<DataSegment> segments = List.of(
        DataSegment.builder(WIKI_SEGMENT).indexingStateFingerprint(expectedFingerprint).build(),
        DataSegment.builder(WIKI_SEGMENT_2).indexingStateFingerprint(expectedFingerprint).build()
    );

    final CompactionStatus status = CompactionStatus.compute(
        CompactionCandidate.from(segments, null),
        compactionConfig,
        fingerprintMapper
    );
    Assert.assertTrue(status.isComplete());
  }

  @Test
  public void test_evaluate_needsCompactionWhenNonFingerprintedSegmentsFailChecksOnLastCompactionState()
  {
    final DataSourceCompactionConfig compactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null))
        .build();

    CompactionState expectedState = compactionConfig.toCompactionState();
    String expectedFingerprint = fingerprintMapper.generateFingerprint(TestDataSource.WIKI, expectedState);

    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, expectedFingerprint, expectedState, DateTimes.nowUtc());
    syncCacheFromManager();

    List<DataSegment> segments = List.of(
        DataSegment.builder(WIKI_SEGMENT).indexingStateFingerprint(expectedFingerprint).build(),
        DataSegment.builder(WIKI_SEGMENT_2).indexingStateFingerprint(null).lastCompactionState(createCompactionStateWithGranularity(Granularities.HOUR)).build()
    );


    verifyEvaluationNeedsCompactionBecauseWithCustomSegments(
        CompactionCandidate.from(segments, null),
        compactionConfig,
        "'segmentGranularity' mismatch: required[DAY], current[HOUR]"
    );
  }

  @Test
  public void test_evaluate_noCompactionWhenNonFingerprintedSegmentsPassChecksOnLastCompactionState()
  {
    final DataSourceCompactionConfig compactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null))
        .build();

    CompactionState expectedState = compactionConfig.toCompactionState();

    String expectedFingerprint = fingerprintMapper.generateFingerprint(TestDataSource.WIKI, expectedState);

    List<DataSegment> segments = List.of(
        DataSegment.builder(WIKI_SEGMENT).indexingStateFingerprint(expectedFingerprint).build(),
        DataSegment.builder(WIKI_SEGMENT_2).indexingStateFingerprint(null).lastCompactionState(createCompactionStateWithGranularity(Granularities.DAY)).build()
    );

    final CompactionStatus status = CompactionStatus.compute(
        CompactionCandidate.from(segments, null),
        compactionConfig,
        fingerprintMapper
    );
    Assert.assertTrue(status.isComplete());
  }

  // ============================
  // SKIPPED status tests
  // ============================

  @Test
  public void test_evaluate_isSkippedWhenInputBytesExceedLimit()
  {
    // Two segments with 100MB each = 200MB total
    // inputSegmentSizeBytes is 150MB, so should be skipped
    final DataSourceCompactionConfig compactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(TestDataSource.WIKI)
        .withInputSegmentSizeBytes(150_000_000L)
        .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null))
        .build();

    final CompactionState lastCompactionState = createCompactionStateWithGranularity(Granularities.HOUR);
    List<DataSegment> segments = List.of(
        DataSegment.builder(WIKI_SEGMENT).lastCompactionState(lastCompactionState).build(),
        DataSegment.builder(WIKI_SEGMENT_2).lastCompactionState(lastCompactionState).build()
    );

    final CompactionStatus status = CompactionStatus.compute(
        CompactionCandidate.from(segments, null),
        compactionConfig,
        fingerprintMapper
    );

    Assert.assertFalse(status.isComplete());
    Assert.assertTrue(status.isSkipped());
    Assert.assertTrue(status.getReason().contains("'inputSegmentSize' exceeded"));
    Assert.assertTrue(status.getReason().contains("200000000"));
    Assert.assertTrue(status.getReason().contains("150000000"));
  }

  /**
   * Verify that the evaluation indicates compaction is needed for the expected reason.
   * Allows customization of the segments in the compaction candidate.
   */
  private void verifyEvaluationNeedsCompactionBecauseWithCustomSegments(
      CompactionCandidate candidate,
      DataSourceCompactionConfig compactionConfig,
      String expectedReason
  )
  {
    final CompactionStatus status = CompactionStatus.compute(
        candidate,
        compactionConfig,
        fingerprintMapper
    );

    Assert.assertFalse(status.isComplete());
    Assert.assertEquals(expectedReason, status.getReason());
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
        CompactionCandidate.from(List.of(segment), null),
        compactionConfig,
        fingerprintMapper
    );

    Assert.assertFalse(status.isComplete());
    Assert.assertEquals(expectedReason, status.getReason());
  }

  private static DataSourceCompactionConfig createCompactionConfig(
      PartitionsSpec partitionsSpec
  )
  {
    return InlineSchemaDataSourceCompactionConfig.builder()
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

  /**
   * Simple helper to create a CompactionState with only segmentGranularity set
   */
  private static CompactionState createCompactionStateWithGranularity(Granularity segmentGranularity)
  {
    return new CompactionState(
        null,
        null,
        null,
        null,
        IndexSpec.getDefault(),
        new UniformGranularitySpec(segmentGranularity, null, null, null),
        null
    );
  }

  // ============================
  // computeRequiredSetOfFilterRulesForCandidate tests
  // ============================

  @Test
  public void testComputeRequiredFilters_SingleFilter_NotOrFilter_ReturnsAsIs()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    NotDimFilter expectedFilter = new NotDimFilter(filterA);

    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");

    NotDimFilter result = CompactionStatus.computeRequiredSetOfFilterRulesForCandidate(
        candidate,
        expectedFilter,
        fingerprintMapper
    );

    Assert.assertEquals(expectedFilter, result);
  }

  @Test
  public void testComputeRequiredFilters_AllFiltersAlreadyApplied_ReturnsNull()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    DimFilter filterC = new SelectorDimFilter("country", "FR", null);

    CompactionState state = createStateWithFilters("fp1", filterA, filterB, filterC);
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp1", state, DateTimes.nowUtc());
    syncCacheFromManager();

    NotDimFilter expectedFilter = new NotDimFilter(new OrDimFilter(Arrays.asList(filterA, filterB, filterC)));
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");

    NotDimFilter result = CompactionStatus.computeRequiredSetOfFilterRulesForCandidate(
        candidate,
        expectedFilter,
        fingerprintMapper
    );

    Assert.assertNull(result);
  }

  @Test
  public void testComputeRequiredFilters_NoFiltersApplied_ReturnsAllExpected()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    DimFilter filterC = new SelectorDimFilter("country", "FR", null);

    CompactionState state = createStateWithoutFilters("fp1");
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp1", state, DateTimes.nowUtc());
    syncCacheFromManager();

    NotDimFilter expectedFilter = new NotDimFilter(new OrDimFilter(Arrays.asList(filterA, filterB, filterC)));
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");

    NotDimFilter result = CompactionStatus.computeRequiredSetOfFilterRulesForCandidate(
        candidate,
        expectedFilter,
        fingerprintMapper
    );

    OrDimFilter innerOr = (OrDimFilter) result.getField();
    Assert.assertEquals(3, innerOr.getFields().size());
    Assert.assertTrue(innerOr.getFields().containsAll(Arrays.asList(filterA, filterB, filterC)));
  }

  @Test
  public void testComputeRequiredFilters_PartiallyApplied_ReturnsDelta()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    DimFilter filterC = new SelectorDimFilter("country", "FR", null);
    DimFilter filterD = new SelectorDimFilter("country", "DE", null);

    CompactionState state = createStateWithFilters("fp1", filterA, filterB);
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp1", state, DateTimes.nowUtc());
    syncCacheFromManager();

    NotDimFilter expectedFilter = new NotDimFilter(
        new OrDimFilter(Arrays.asList(filterA, filterB, filterC, filterD))
    );
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");

    NotDimFilter result = CompactionStatus.computeRequiredSetOfFilterRulesForCandidate(
        candidate,
        expectedFilter,
        fingerprintMapper
    );

    OrDimFilter innerOr = (OrDimFilter) result.getField();
    Set<DimFilter> resultSet = new HashSet<>(innerOr.getFields());
    Set<DimFilter> expectedSet = new HashSet<>(Arrays.asList(filterC, filterD));

    Assert.assertEquals(expectedSet, resultSet);
  }

  @Test
  public void testComputeRequiredFilters_MultipleFingerprints_UnionOfMissing()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    DimFilter filterC = new SelectorDimFilter("country", "FR", null);
    DimFilter filterD = new SelectorDimFilter("country", "DE", null);

    CompactionState state1 = createStateWithFilters("fp1", filterA, filterB);
    CompactionState state2 = createStateWithFilters("fp2", filterA, filterC);
    DateTime now = DateTimes.nowUtc();
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp1", state1, now);
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp2", state2, now);
    syncCacheFromManager();

    NotDimFilter expectedFilter = new NotDimFilter(
        new OrDimFilter(Arrays.asList(filterA, filterB, filterC, filterD))
    );
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1", "fp2");

    NotDimFilter result = CompactionStatus.computeRequiredSetOfFilterRulesForCandidate(
        candidate,
        expectedFilter,
        fingerprintMapper
    );

    OrDimFilter innerOr = (OrDimFilter) result.getField();
    Set<DimFilter> resultSet = new HashSet<>(innerOr.getFields());
    Set<DimFilter> expectedSet = new HashSet<>(Arrays.asList(filterB, filterC, filterD));

    Assert.assertEquals(expectedSet, resultSet);
  }

  @Test
  public void testComputeRequiredFilters_MultipleFingerprints_NoDuplicates()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    DimFilter filterC = new SelectorDimFilter("country", "FR", null);

    CompactionState state1 = createStateWithFilters("fp1", filterA);
    CompactionState state2 = createStateWithFilters("fp2", filterA);
    DateTime now = DateTimes.nowUtc();
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp1", state1, now);
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp2", state2, now);
    syncCacheFromManager();

    NotDimFilter expectedFilter = new NotDimFilter(
        new OrDimFilter(Arrays.asList(filterA, filterB, filterC))
    );
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1", "fp2");

    NotDimFilter result = CompactionStatus.computeRequiredSetOfFilterRulesForCandidate(
        candidate,
        expectedFilter,
        fingerprintMapper
    );

    OrDimFilter innerOr = (OrDimFilter) result.getField();
    Set<DimFilter> resultSet = new HashSet<>(innerOr.getFields());

    Assert.assertEquals(2, resultSet.size());
    Assert.assertTrue(resultSet.containsAll(Arrays.asList(filterB, filterC)));
  }

  @Test
  public void testComputeRequiredFilters_MissingCompactionState_ReturnsAllFilters()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    DimFilter filterC = new SelectorDimFilter("country", "FR", null);

    // No state persisted for fp1
    NotDimFilter expectedFilter = new NotDimFilter(
        new OrDimFilter(Arrays.asList(filterA, filterB, filterC))
    );
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");

    NotDimFilter result = CompactionStatus.computeRequiredSetOfFilterRulesForCandidate(
        candidate,
        expectedFilter,
        fingerprintMapper
    );

    Assert.assertEquals(expectedFilter, result);
  }

  @Test
  public void testComputeRequiredFilters_TransformSpecWithSingleFilter()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    DimFilter filterC = new SelectorDimFilter("country", "FR", null);

    CompactionState state = createStateWithSingleFilter("fp1", filterA);
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp1", state, DateTimes.nowUtc());
    syncCacheFromManager();

    NotDimFilter expectedFilter = new NotDimFilter(
        new OrDimFilter(Arrays.asList(filterA, filterB, filterC))
    );
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");

    NotDimFilter result = CompactionStatus.computeRequiredSetOfFilterRulesForCandidate(
        candidate,
        expectedFilter,
        fingerprintMapper
    );

    OrDimFilter innerOr = (OrDimFilter) result.getField();
    Assert.assertEquals(2, innerOr.getFields().size());
    Assert.assertTrue(innerOr.getFields().containsAll(Arrays.asList(filterB, filterC)));
  }

  @Test
  public void testComputeRequiredFilters_SegmentsWithNoFingerprints()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    DimFilter filterC = new SelectorDimFilter("country", "FR", null);

    CompactionCandidate candidate = createCandidateWithNullFingerprints(3);

    NotDimFilter expectedFilter = new NotDimFilter(
        new OrDimFilter(Arrays.asList(filterA, filterB, filterC))
    );

    NotDimFilter result = CompactionStatus.computeRequiredSetOfFilterRulesForCandidate(
        candidate,
        expectedFilter,
        fingerprintMapper
    );

    Assert.assertEquals(expectedFilter, result);
  }

  // Helper methods for filter tests

  private CompactionCandidate createCandidateWithFingerprints(String... fingerprints)
  {
    List<DataSegment> segments = Arrays.stream(fingerprints)
        .map(fp -> DataSegment.builder(WIKI_SEGMENT).indexingStateFingerprint(fp).build())
        .collect(Collectors.toList());
    return CompactionCandidate.from(segments, null);
  }

  private CompactionCandidate createCandidateWithNullFingerprints(int count)
  {
    List<DataSegment> segments = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      segments.add(DataSegment.builder(WIKI_SEGMENT).indexingStateFingerprint(null).build());
    }
    return CompactionCandidate.from(segments, null);
  }

  private CompactionState createStateWithFilters(String fingerprint, DimFilter... filters)
  {
    OrDimFilter orFilter = new OrDimFilter(Arrays.asList(filters));
    NotDimFilter notFilter = new NotDimFilter(orFilter);
    CompactionTransformSpec transformSpec = new CompactionTransformSpec(notFilter);

    return new CompactionState(
        null,
        null,
        null,
        transformSpec,
        IndexSpec.getDefault(),
        null,
        null
    );
  }

  private CompactionState createStateWithSingleFilter(String fingerprint, DimFilter filter)
  {
    NotDimFilter notFilter = new NotDimFilter(filter);
    CompactionTransformSpec transformSpec = new CompactionTransformSpec(notFilter);

    return new CompactionState(
        null,
        null,
        null,
        transformSpec,
        IndexSpec.getDefault(),
        null,
        null
    );
  }

  private CompactionState createStateWithoutFilters(String fingerprint)
  {
    return new CompactionState(
        null,
        null,
        null,
        null,
        IndexSpec.getDefault(),
        null,
        null
    );
  }
}
