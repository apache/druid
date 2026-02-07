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
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.metadata.DefaultIndexingStateFingerprintMapper;
import org.apache.druid.segment.metadata.HeapMemoryIndexingStateStorage;
import org.apache.druid.segment.metadata.IndexingStateCache;
import org.apache.druid.segment.metadata.IndexingStateFingerprintMapper;
import org.apache.druid.segment.nested.NestedCommonFormatColumnFormatSpec;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

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
    Assert.assertNull(CompactionStatus.findPartitionsSpecFromConfig(tuningConfig));
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
        CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(config))
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
    verifyCompactionIsEligibleBecause(
        null,
        InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        "not compacted yet"
    );
  }

  @Test
  public void testStatusWhenLastCompactionStateIsEmpty()
  {
    final PartitionsSpec requiredPartitionsSpec = new DynamicPartitionsSpec(5_000_000, null);
    verifyCompactionIsEligibleBecause(
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

    verifyCompactionIsEligibleBecause(
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

    verifyCompactionIsEligibleBecause(
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

    verifyCompactionIsEligibleBecause(
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
    final CompactionStatus status = CompactionStatus.evaluate(
        CompactionCandidate.ProposedCompaction.from(List.of(segment), Granularities.HOUR),
        compactionConfig,
        fingerprintMapper
    );
    Assert.assertEquals(CompactionStatus.State.COMPLETE, status.getState());
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
    final CompactionStatus status = CompactionStatus.evaluate(
        CompactionCandidate.ProposedCompaction.from(List.of(segment), Granularities.HOUR),
        compactionConfig,
        fingerprintMapper
    );
    Assert.assertEquals(CompactionStatus.COMPLETE, status);
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
    final CompactionStatus status = CompactionStatus.evaluate(
        CompactionCandidate.ProposedCompaction.from(List.of(segment), Granularities.HOUR),
        compactionConfig,
        fingerprintMapper
    );
    Assert.assertEquals(CompactionStatus.State.ELIGIBLE, status.getState());
    Assert.assertTrue(status.getReason().contains("'projections' mismatch"));
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
    final CompactionStatus status = CompactionStatus.evaluate(
        CompactionCandidate.ProposedCompaction.from(List.of(segment), null),
        compactionConfig,
        fingerprintMapper
    );
    Assert.assertEquals(CompactionStatus.COMPLETE, status);
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
    final CompactionStatus status = CompactionStatus.evaluate(
        CompactionCandidate.ProposedCompaction.from(List.of(segment), null),
        compactionConfig,
        fingerprintMapper
    );
    Assert.assertEquals(CompactionStatus.State.ELIGIBLE, status.getState());
    Assert.assertTrue(status.getReason().contains("'dimensionsSpec' mismatch"));
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
        CompactionCandidate.ProposedCompaction.from(segments, null),
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
        CompactionCandidate.ProposedCompaction.from(segments, null),
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

    final CompactionStatus status = CompactionStatus.evaluate(
        CompactionCandidate.ProposedCompaction.from(segments, null),
        compactionConfig,
        fingerprintMapper
    );
    Assert.assertEquals(CompactionStatus.COMPLETE, status);
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
        CompactionCandidate.ProposedCompaction.from(segments, null),
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

    final CompactionStatus status = CompactionStatus.evaluate(
        CompactionCandidate.ProposedCompaction.from(segments, null),
        compactionConfig,
        fingerprintMapper
    );
    Assert.assertEquals(CompactionStatus.COMPLETE, status);
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
        CompactionCandidate.ProposedCompaction.from(segments, null),
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

    final CompactionStatus status = CompactionStatus.evaluate(
        CompactionCandidate.ProposedCompaction.from(segments, null),
        compactionConfig,
        fingerprintMapper
    );
    Assert.assertEquals(CompactionStatus.COMPLETE, status);
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

    final CompactionStatus status = CompactionStatus.evaluate(
        CompactionCandidate.ProposedCompaction.from(segments, null),
        compactionConfig,
        fingerprintMapper
    );

    Assert.assertFalse(status.getState().equals(CompactionStatus.State.ELIGIBLE));
    Assert.assertTrue(status.getReason().contains("'inputSegmentSize' exceeded"));
    Assert.assertTrue(status.getReason().contains("200000000"));
    Assert.assertTrue(status.getReason().contains("150000000"));
  }

  /**
   * Verify that the evaluation indicates compaction is needed for the expected reason.
   * Allows customization of the segments in the compaction candidate.
   */
  private void verifyEvaluationNeedsCompactionBecauseWithCustomSegments(
      CompactionCandidate.ProposedCompaction proposedCompaction,
      DataSourceCompactionConfig compactionConfig,
      String expectedReason
  )
  {
    final CompactionStatus status = CompactionStatus.evaluate(
        proposedCompaction,
        compactionConfig,
        fingerprintMapper
    );

    Assert.assertEquals(CompactionStatus.State.ELIGIBLE, status.getState());
    Assert.assertEquals(expectedReason, status.getReason());
  }

  private void verifyCompactionIsEligibleBecause(
      CompactionState lastCompactionState,
      DataSourceCompactionConfig compactionConfig,
      String expectedReason
  )
  {
    final DataSegment segment
        = DataSegment.builder(WIKI_SEGMENT)
                     .lastCompactionState(lastCompactionState)
                     .build();
    final CompactionStatus status = CompactionStatus.evaluate(
        CompactionCandidate.ProposedCompaction.from(List.of(segment), null),
        compactionConfig,
        fingerprintMapper
    );

    Assert.assertEquals(CompactionStatus.State.ELIGIBLE, status.getState());
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
}
