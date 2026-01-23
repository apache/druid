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

import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.metadata.DefaultIndexingStateFingerprintMapper;
import org.apache.druid.segment.metadata.HeapMemoryIndexingStateStorage;
import org.apache.druid.segment.metadata.IndexingStateCache;
import org.apache.druid.segment.metadata.IndexingStateFingerprintMapper;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ReindexingFilterRuleOptimizerTest
{
  private static final DataSegment WIKI_SEGMENT
      = DataSegment.builder(SegmentId.of(TestDataSource.WIKI, Intervals.of("2013-01-01/PT1H"), "v1", 0))
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

  @Test
  public void testComputeRequiredFilters_SingleFilter_NotOrFilter_ReturnsAsIs()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    NotDimFilter expectedFilter = new NotDimFilter(filterA);

    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");

    NotDimFilter result = ReindexingFilterRuleOptimizer.computeRequiredSetOfFilterRulesForCandidate(
        candidate,
        expectedFilter,
        fingerprintMapper
    );

    Assert.assertEquals(expectedFilter, result);
  }

  @Test
  public void test_computeRequiredSetOfFilterRulesForCandidate_oneFilter_nothingToApply()
  {
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    CompactionState state = createStateWithFilters(filterB);
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp1", state, DateTimes.nowUtc());
    syncCacheFromManager();
    NotDimFilter expectedFilter = new NotDimFilter(filterB);
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");

    NotDimFilter result = ReindexingFilterRuleOptimizer.computeRequiredSetOfFilterRulesForCandidate(
        candidate,
        expectedFilter,
        fingerprintMapper
    );

    Assert.assertNull(result);
  }

  @Test
  public void testComputeRequiredFilters_AllFiltersAlreadyApplied_ReturnsNull()
  {
    DimFilter filterA = new SelectorDimFilter("country", "US", null);
    DimFilter filterB = new SelectorDimFilter("country", "UK", null);
    DimFilter filterC = new SelectorDimFilter("country", "FR", null);

    CompactionState state = createStateWithFilters(filterA, filterB, filterC);
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp1", state, DateTimes.nowUtc());
    syncCacheFromManager();

    NotDimFilter expectedFilter = new NotDimFilter(new OrDimFilter(Arrays.asList(filterA, filterB, filterC)));
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");

    NotDimFilter result = ReindexingFilterRuleOptimizer.computeRequiredSetOfFilterRulesForCandidate(
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

    CompactionState state = createStateWithoutFilters();
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp1", state, DateTimes.nowUtc());
    syncCacheFromManager();

    NotDimFilter expectedFilter = new NotDimFilter(new OrDimFilter(Arrays.asList(filterA, filterB, filterC)));
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");

    NotDimFilter result = ReindexingFilterRuleOptimizer.computeRequiredSetOfFilterRulesForCandidate(
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

    CompactionState state = createStateWithFilters(filterA, filterB);
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp1", state, DateTimes.nowUtc());
    syncCacheFromManager();

    NotDimFilter expectedFilter = new NotDimFilter(
        new OrDimFilter(Arrays.asList(filterA, filterB, filterC, filterD))
    );
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");

    NotDimFilter result = ReindexingFilterRuleOptimizer.computeRequiredSetOfFilterRulesForCandidate(
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

    NotDimFilter result = ReindexingFilterRuleOptimizer.computeRequiredSetOfFilterRulesForCandidate(
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

    NotDimFilter result = ReindexingFilterRuleOptimizer.computeRequiredSetOfFilterRulesForCandidate(
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

    NotDimFilter result = ReindexingFilterRuleOptimizer.computeRequiredSetOfFilterRulesForCandidate(
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

    CompactionState state = createStateWithSingleFilter(filterA);
    indexingStateStorage.upsertIndexingState(TestDataSource.WIKI, "fp1", state, DateTimes.nowUtc());
    syncCacheFromManager();

    NotDimFilter expectedFilter = new NotDimFilter(
        new OrDimFilter(Arrays.asList(filterA, filterB, filterC))
    );
    CompactionCandidate candidate = createCandidateWithFingerprints("fp1");

    NotDimFilter result = ReindexingFilterRuleOptimizer.computeRequiredSetOfFilterRulesForCandidate(
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

    NotDimFilter result = ReindexingFilterRuleOptimizer.computeRequiredSetOfFilterRulesForCandidate(
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

  private CompactionState createStateWithFilters(DimFilter... filters)
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

  private CompactionState createStateWithSingleFilter(DimFilter filter)
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

  /**
   * Helper to sync the cache with states stored in the manager (for tests that persist states).
   */
  private void syncCacheFromManager()
  {
    indexingStateCache.resetIndexingStatesForPublishedSegments(indexingStateStorage.getAllStoredStates());
  }
}
