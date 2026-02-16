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

package org.apache.druid.indexing.compact;

import com.google.common.collect.ImmutableList;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.metadata.DefaultIndexingStateFingerprintMapper;
import org.apache.druid.segment.metadata.HeapMemoryIndexingStateStorage;
import org.apache.druid.segment.metadata.IndexingStateCache;
import org.apache.druid.segment.metadata.IndexingStateFingerprintMapper;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.server.compaction.CompactionStatus;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ReindexingDeletionRuleOptimizerTest
{
  private static final DataSegment WIKI_SEGMENT
      = DataSegment.builder(SegmentId.of(TestDataSource.WIKI, Intervals.of("2013-01-01/PT1H"), "v1", 0))
                   .size(100_000_000L)
                   .build();

  private HeapMemoryIndexingStateStorage indexingStateStorage;
  private IndexingStateCache indexingStateCache;
  private IndexingStateFingerprintMapper fingerprintMapper;
  private ReindexingDeletionRuleOptimizer optimizer;

  @Before
  public void setUp()
  {
    indexingStateStorage = new HeapMemoryIndexingStateStorage();
    indexingStateCache = new IndexingStateCache();
    fingerprintMapper = new DefaultIndexingStateFingerprintMapper(
        indexingStateCache,
        new DefaultObjectMapper()
    );
    optimizer = new ReindexingDeletionRuleOptimizer();
  }

  @Test
  public void testOptimize_SingleFilter_NotOrFilter_NoFingerprints_ReturnsUnchanged()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    NotDimFilter expectedFilter = new NotDimFilter(filterA);

    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");
    InlineSchemaDataSourceCompactionConfig config = createConfigWithFilter(expectedFilter, null);
    CompactionJobParams params = createParams();

    DataSourceCompactionConfig result = optimizer.optimizeConfig(config, candidate, params);

    // No state for fp1, so config should be unchanged
    Assert.assertEquals(expectedFilter, result.getTransformSpec().getFilter());
  }

  @Test
  public void testOptimize_SingleFilter_AlreadyApplied_RemovesTransformSpec()
  {
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    CompactionState state = createStateWithSingleFilter(filterB);
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp1", state, DateTimes.nowUtc());
    syncCacheFromManager();

    NotDimFilter expectedFilter = new NotDimFilter(filterB);
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");
    InlineSchemaDataSourceCompactionConfig config = createConfigWithFilter(expectedFilter, null);
    CompactionJobParams params = createParams();

    DataSourceCompactionConfig result = optimizer.optimizeConfig(config, candidate, params);

    // All filters optimized away, transform spec should be null
    Assert.assertNull(result.getTransformSpec());
  }

  @Test
  public void testOptimize_AllFiltersAlreadyApplied_RemovesTransformSpec()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    DimFilter filterC = new SelectorDimFilter("country", "FR", null);

    CompactionState state = createStateWithFilters(filterA, filterB, filterC);
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp1", state, DateTimes.nowUtc());
    syncCacheFromManager();

    NotDimFilter expectedFilter = new NotDimFilter(new OrDimFilter(Arrays.asList(filterA, filterB, filterC)));
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");
    InlineSchemaDataSourceCompactionConfig config = createConfigWithFilter(expectedFilter, null);
    CompactionJobParams params = createParams();

    DataSourceCompactionConfig result = optimizer.optimizeConfig(config, candidate, params);

    // All filters already applied, transform spec should be removed
    Assert.assertNull(result.getTransformSpec());
  }

  @Test
  public void testOptimize_NoFiltersApplied_ReturnsAllExpected()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    DimFilter filterC = new SelectorDimFilter("country", "FR", null);

    CompactionState state = createStateWithoutFilters();
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp1", state, DateTimes.nowUtc());
    syncCacheFromManager();

    NotDimFilter expectedFilter = new NotDimFilter(new OrDimFilter(Arrays.asList(filterA, filterB, filterC)));
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");
    InlineSchemaDataSourceCompactionConfig config = createConfigWithFilter(expectedFilter, null);
    CompactionJobParams params = createParams();

    DataSourceCompactionConfig result = optimizer.optimizeConfig(config, candidate, params);

    // No filters were applied, so all should remain
    NotDimFilter resultFilter = (NotDimFilter) result.getTransformSpec().getFilter();
    OrDimFilter innerOr = (OrDimFilter) resultFilter.getField();
    Assert.assertEquals(3, innerOr.getFields().size());
    Assert.assertTrue(innerOr.getFields().containsAll(Arrays.asList(filterA, filterB, filterC)));
  }

  @Test
  public void testOptimize_PartiallyApplied_ReturnsDelta()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    DimFilter filterC = new SelectorDimFilter("country", "FR", null);
    DimFilter filterD = new SelectorDimFilter("country", "DE", null);

    CompactionState state = createStateWithFilters(filterA, filterB);
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp1", state, DateTimes.nowUtc());
    syncCacheFromManager();

    NotDimFilter expectedFilter = new NotDimFilter(
        new OrDimFilter(Arrays.asList(filterA, filterB, filterC, filterD))
    );
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");
    InlineSchemaDataSourceCompactionConfig config = createConfigWithFilter(expectedFilter, null);
    CompactionJobParams params = createParams();

    DataSourceCompactionConfig result = optimizer.optimizeConfig(config, candidate, params);

    // Only C and D should remain (A and B were already applied)
    NotDimFilter resultFilter = (NotDimFilter) result.getTransformSpec().getFilter();
    OrDimFilter innerOr = (OrDimFilter) resultFilter.getField();
    Set<DimFilter> resultSet = new HashSet<>(innerOr.getFields());
    Set<DimFilter> expectedSet = new HashSet<>(Arrays.asList(filterC, filterD));

    Assert.assertEquals(expectedSet, resultSet);
  }

  @Test
  public void testOptimize_MultipleFingerprints_UnionOfMissing()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    DimFilter filterC = new SelectorDimFilter("country", "FR", null);
    DimFilter filterD = new SelectorDimFilter("country", "DE", null);

    CompactionState state1 = createStateWithFilters(filterA, filterB);
    CompactionState state2 = createStateWithFilters(filterA, filterC);
    DateTime now = DateTimes.nowUtc();
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp1", state1, now);
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp2", state2, now);
    syncCacheFromManager();

    NotDimFilter expectedFilter = new NotDimFilter(
        new OrDimFilter(Arrays.asList(filterA, filterB, filterC, filterD))
    );
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1", "fp2");
    InlineSchemaDataSourceCompactionConfig config = createConfigWithFilter(expectedFilter, null);
    CompactionJobParams params = createParams();

    DataSourceCompactionConfig result = optimizer.optimizeConfig(config, candidate, params);

    // fp1 has A,B applied; fp2 has A,C applied
    // Union of missing: B (missing from fp2), C (missing from fp1), D (missing from both)
    NotDimFilter resultFilter = (NotDimFilter) result.getTransformSpec().getFilter();
    OrDimFilter innerOr = (OrDimFilter) resultFilter.getField();
    Set<DimFilter> resultSet = new HashSet<>(innerOr.getFields());
    Set<DimFilter> expectedSet = new HashSet<>(Arrays.asList(filterB, filterC, filterD));

    Assert.assertEquals(expectedSet, resultSet);
  }

  @Test
  public void testOptimize_MultipleFingerprints_NoDuplicates()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    DimFilter filterC = new SelectorDimFilter("country", "FR", null);

    CompactionState state1 = createStateWithFilters(filterA);
    CompactionState state2 = createStateWithFilters(filterA);
    DateTime now = DateTimes.nowUtc();
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp1", state1, now);
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp2", state2, now);
    syncCacheFromManager();

    NotDimFilter expectedFilter = new NotDimFilter(
        new OrDimFilter(Arrays.asList(filterA, filterB, filterC))
    );
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1", "fp2");
    InlineSchemaDataSourceCompactionConfig config = createConfigWithFilter(expectedFilter, null);
    CompactionJobParams params = createParams();

    DataSourceCompactionConfig result = optimizer.optimizeConfig(config, candidate, params);

    // Both fingerprints have A applied, so only B and C remain
    NotDimFilter resultFilter = (NotDimFilter) result.getTransformSpec().getFilter();
    OrDimFilter innerOr = (OrDimFilter) resultFilter.getField();
    Set<DimFilter> resultSet = new HashSet<>(innerOr.getFields());

    Assert.assertEquals(2, resultSet.size());
    Assert.assertTrue(resultSet.containsAll(Arrays.asList(filterB, filterC)));
  }

  @Test
  public void testOptimize_MissingCompactionState_ReturnsAllFilters()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    DimFilter filterC = new SelectorDimFilter("country", "FR", null);

    // No state persisted for fp1
    NotDimFilter expectedFilter = new NotDimFilter(
        new OrDimFilter(Arrays.asList(filterA, filterB, filterC))
    );
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");
    InlineSchemaDataSourceCompactionConfig config = createConfigWithFilter(expectedFilter, null);
    CompactionJobParams params = createParams();

    DataSourceCompactionConfig result = optimizer.optimizeConfig(config, candidate, params);

    // No state available, all filters should remain
    Assert.assertEquals(expectedFilter, result.getTransformSpec().getFilter());
  }

  @Test
  public void testOptimize_TransformSpecWithSingleFilter()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    DimFilter filterC = new SelectorDimFilter("country", "FR", null);

    CompactionState state = createStateWithSingleFilter(filterA);
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp1", state, DateTimes.nowUtc());
    syncCacheFromManager();

    NotDimFilter expectedFilter = new NotDimFilter(
        new OrDimFilter(Arrays.asList(filterA, filterB, filterC))
    );
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");
    InlineSchemaDataSourceCompactionConfig config = createConfigWithFilter(expectedFilter, null);
    CompactionJobParams params = createParams();

    DataSourceCompactionConfig result = optimizer.optimizeConfig(config, candidate, params);

    // A was already applied, only B and C should remain
    NotDimFilter resultFilter = (NotDimFilter) result.getTransformSpec().getFilter();
    OrDimFilter innerOr = (OrDimFilter) resultFilter.getField();
    Assert.assertEquals(2, innerOr.getFields().size());
    Assert.assertTrue(innerOr.getFields().containsAll(Arrays.asList(filterB, filterC)));
  }

  @Test
  public void testOptimize_SegmentsWithNoFingerprints()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    DimFilter filterC = new SelectorDimFilter("country", "FR", null);

    CompactionCandidate candidate = createCandidateWithNullFingerprints(3);

    NotDimFilter expectedFilter = new NotDimFilter(
        new OrDimFilter(Arrays.asList(filterA, filterB, filterC))
    );
    InlineSchemaDataSourceCompactionConfig config = createConfigWithFilter(expectedFilter, null);
    CompactionJobParams params = createParams();

    DataSourceCompactionConfig result = optimizer.optimizeConfig(config, candidate, params);

    // No fingerprints, all filters should remain
    Assert.assertEquals(expectedFilter, result.getTransformSpec().getFilter());
  }




  // Helper methods

  private InlineSchemaDataSourceCompactionConfig createConfigWithFilter(
      @Nullable NotDimFilter filter,
      @Nullable VirtualColumns virtualColumns
  )
  {
    CompactionTransformSpec transformSpec = filter == null && virtualColumns == null
                                            ? null
                                            : new CompactionTransformSpec(filter, virtualColumns);

    return InlineSchemaDataSourceCompactionConfig.builder()
        .forDataSource(TestDataSource.WIKI)
        .withTransformSpec(transformSpec)
        .build();
  }

  private CompactionJobParams createParams()
  {
    CompactionJobParams mockParams = EasyMock.createMock(CompactionJobParams.class);
    EasyMock.expect(mockParams.getFingerprintMapper()).andReturn(fingerprintMapper).anyTimes();
    EasyMock.replay(mockParams);
    return mockParams;
  }

  private CompactionCandidate createCandidateWithFingerprints(String... fingerprints)
  {
    List<DataSegment> segments = Arrays.stream(fingerprints)
                                       .map(fp -> DataSegment.builder(WIKI_SEGMENT).indexingStateFingerprint(fp).build())
                                       .collect(Collectors.toList());
    return CompactionCandidate.from(segments, null)
        .withCurrentStatus(CompactionStatus.pending("segments need compaction"));
  }

  private CompactionCandidate createCandidateWithNullFingerprints(int count)
  {
    List<DataSegment> segments = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      segments.add(DataSegment.builder(WIKI_SEGMENT).indexingStateFingerprint(null).build());
    }
    return CompactionCandidate.from(segments, null)
        .withCurrentStatus(CompactionStatus.pending("segments need compaction"));
  }

  private CompactionState createStateWithFilters(DimFilter... filters)
  {
    OrDimFilter orFilter = new OrDimFilter(Arrays.asList(filters));
    NotDimFilter notFilter = new NotDimFilter(orFilter);
    CompactionTransformSpec transformSpec = new CompactionTransformSpec(notFilter, null);

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

  private CompactionState createStateWithSingleFilter(DimFilter filter)
  {
    NotDimFilter notFilter = new NotDimFilter(filter);
    CompactionTransformSpec transformSpec = new CompactionTransformSpec(notFilter, null);

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

  private CompactionState createStateWithoutFilters()
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

  @Test
  public void testOptimize_FilterVirtualColumns_SomeColumnsReferenced()
  {
    // Create virtual columns vc1, vc2, vc3
    VirtualColumns virtualColumns = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn("vc1", "col1 + 1", ColumnType.LONG, TestExprMacroTable.INSTANCE),
            new ExpressionVirtualColumn("vc2", "col2 + 2", ColumnType.LONG, TestExprMacroTable.INSTANCE),
            new ExpressionVirtualColumn("vc3", "col3 + 3", ColumnType.LONG, TestExprMacroTable.INSTANCE)
        )
    );

    // Create a filter that only references vc1 and vc3 (vc2 is unreferenced)
    DimFilter filter = new OrDimFilter(
        Arrays.asList(
            new SelectorDimFilter("vc1", "value1", null),
            new SelectorDimFilter("vc3", "value3", null)
        )
    );
    NotDimFilter notFilter = new NotDimFilter(filter);

    // Candidate has no filters applied, so all filters remain
    CompactionCandidate candidate = createCandidateWithNullFingerprints(1);
    InlineSchemaDataSourceCompactionConfig config = createConfigWithFilter(notFilter, virtualColumns);
    CompactionJobParams params = createParams();

    DataSourceCompactionConfig result = optimizer.optimizeConfig(config, candidate, params);

    // Filter remains, but vc2 should be filtered out
    VirtualColumns resultVCs = result.getTransformSpec().getVirtualColumns();
    Assert.assertNotNull(resultVCs);
    Assert.assertEquals(2, resultVCs.getVirtualColumns().length);

    Set<String> outputNames = new HashSet<>();
    for (org.apache.druid.segment.VirtualColumn vc : resultVCs.getVirtualColumns()) {
      outputNames.add(vc.getOutputName());
    }

    Assert.assertTrue(outputNames.contains("vc1"));
    Assert.assertFalse(outputNames.contains("vc2")); // vc2 should be filtered out
    Assert.assertTrue(outputNames.contains("vc3"));
  }

  @Test
  public void testOptimize_FilterVirtualColumns_NoColumnsReferenced()
  {
    // Create virtual columns
    VirtualColumns virtualColumns = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn("vc1", "col1 + 1", ColumnType.LONG, TestExprMacroTable.INSTANCE),
            new ExpressionVirtualColumn("vc2", "col2 + 2", ColumnType.LONG, TestExprMacroTable.INSTANCE)
        )
    );

    // Create a filter that references a physical column, not virtual columns
    DimFilter filter = new SelectorDimFilter("regularColumn", "value", null);
    NotDimFilter notFilter = new NotDimFilter(filter);

    // Candidate has no filters applied
    CompactionCandidate candidate = createCandidateWithNullFingerprints(1);
    InlineSchemaDataSourceCompactionConfig config = createConfigWithFilter(notFilter, virtualColumns);
    CompactionJobParams params = createParams();

    DataSourceCompactionConfig result = optimizer.optimizeConfig(config, candidate, params);

    // Virtual columns should be null when no virtual columns are referenced
    Assert.assertNull(result.getTransformSpec().getVirtualColumns());
  }

  @Test
  public void testOptimize_CandidateNeverCompacted_NoOptimization()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    NotDimFilter expectedFilter = new NotDimFilter(filterA);

    // Candidate with NEVER_COMPACTED status
    List<DataSegment> segments = new ArrayList<>();
    segments.add(DataSegment.builder(WIKI_SEGMENT).indexingStateFingerprint(null).build());
    CompactionCandidate candidate = CompactionCandidate.from(segments, null)
        .withCurrentStatus(CompactionStatus.pending(CompactionStatus.NEVER_COMPACTED_REASON));

    InlineSchemaDataSourceCompactionConfig config = createConfigWithFilter(expectedFilter, null);
    CompactionJobParams params = createParams();

    DataSourceCompactionConfig result = optimizer.optimizeConfig(config, candidate, params);

    // Should return config unchanged since candidate was never compacted
    Assert.assertSame(config, result);
  }

  @Test
  public void testOptimize_NoTransformSpec_NoOptimization()
  {
    // Config without transform spec
    InlineSchemaDataSourceCompactionConfig config = InlineSchemaDataSourceCompactionConfig.builder()
        .forDataSource(TestDataSource.WIKI)
        .build();

    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");
    CompactionJobParams params = createParams();

    DataSourceCompactionConfig result = optimizer.optimizeConfig(config, candidate, params);

    // Should return config unchanged
    Assert.assertSame(config, result);
  }

  /**
   * Helper to sync the cache with states stored in the manager (for tests that persist states).
   */
  private void syncCacheFromManager()
  {
    indexingStateCache.resetIndexingStatesForPublishedSegments(indexingStateStorage.getAllStoredStates());
  }
}
