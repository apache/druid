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

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.metadata.IndexingStateFingerprintMapper;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.server.compaction.CompactionStatus;
import org.apache.druid.server.compaction.ReindexingDeletionRule;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Optimization utilities for applying {@link ReindexingDeletionRule}s during reindexing
 * <p>
 * When reindexing with {@link ReindexingDeletionRule}s, it is possible that candidate
 * segments have already applied some or all of the deletion rules in previous reindexing runs. Reapplying such rules would
 * be wasteful and redundant. This class provides funcionality to optimize the set of rules to be applied by
 * any given reindexing task.
 */
public class ReindexingDeletionRuleOptimizer implements ReindexingConfigOptimizer
{
  private static final Logger LOG = new Logger(ReindexingDeletionRuleOptimizer.class);

  @Override
  public DataSourceCompactionConfig optimizeConfig(
      DataSourceCompactionConfig config,
      CompactionCandidate candidate,
      CompactionJobParams params
  )
  {
    if (!shouldOptimizeFilterRules(candidate, config)) {
      return config;
    }

    NotDimFilter reducedFilter = computeRequiredSetOfFilterRulesForCandidate(
        candidate,
        (NotDimFilter) config.getTransformSpec().getFilter(),
        params.getFingerprintMapper()
    );

    VirtualColumns reducedVirtualColumns = filterVirtualColumnsForFilter(
        reducedFilter,
        config.getTransformSpec().getVirtualColumns()
    );

    CompactionTransformSpec transformSpec = (reducedFilter == null && reducedVirtualColumns == null)
        ? null
        : new CompactionTransformSpec(reducedFilter, reducedVirtualColumns);

    return ((InlineSchemaDataSourceCompactionConfig) config)
        .toBuilder()
        .withTransformSpec(transformSpec)
        .build();
  }

  /**
   * Computes the required set of deletion rules to be applied for the given {@link CompactionCandidate}.
   * <p>
   * We only want to apply the rules that have not yet been applied to all segments in the candidate. This reduces
   * the amount of work the task needs to do while processing rows during reindexing.
   * </p>
   *
   * @param candidateSegments the {@link CompactionCandidate}
   * @param expectedFilter the expected filter (as a NotDimFilter wrapping an OrDimFilter)
   * @param fingerprintMapper the fingerprint mapper to retrieve applied rules from segment fingerprints
   * @return the set of unapplied deletion rules wrapped in a NotDimFilter, or null if all rules have been applied
   */
  @Nullable
  private NotDimFilter computeRequiredSetOfFilterRulesForCandidate(
      CompactionCandidate candidateSegments,
      NotDimFilter expectedFilter,
      IndexingStateFingerprintMapper fingerprintMapper
  )
  {
    List<DimFilter> expectedFilters;
    if (!(expectedFilter.getField() instanceof OrDimFilter)) {
      expectedFilters = Collections.singletonList(expectedFilter.getField());
    } else {
      expectedFilters = ((OrDimFilter) expectedFilter.getField()).getFields();
    }

    Set<String> uniqueFingerprints = candidateSegments.getSegments().stream()
                                                      .map(DataSegment::getIndexingStateFingerprint)
                                                      .filter(Objects::nonNull)
                                                      .collect(Collectors.toSet());

    if (uniqueFingerprints.isEmpty()) {
      // no fingerprints means that no candidate segments have transforms to compare against. Return all filters eagerly.
      return expectedFilter;
    }

    Set<DimFilter> unappliedRules = new HashSet<>();

    for (String fingerprint : uniqueFingerprints) {
      CompactionState state = fingerprintMapper.getStateForFingerprint(fingerprint).orElse(null);

      if (state == null) {
        // Safety: if state is missing, return all filters eagerly since we can't determine applied filters
        return expectedFilter;
      }

      Set<DimFilter> appliedFilters = extractAppliedFilters(state);

      if (appliedFilters == null) {
        return expectedFilter;
      }

      for (DimFilter expected : expectedFilters) {
        if (!appliedFilters.contains(expected)) {
          unappliedRules.add(expected);
        }
      }
    }

    LOG.debug(
        "Computed [%d] unapplied rules out of [%d] possible rules for candidate",
        unappliedRules.size(),
        expectedFilters.size()
    );

    if (unappliedRules.isEmpty()) {
      return null;
    }

    return new NotDimFilter(new OrDimFilter(new ArrayList<>(unappliedRules)));
  }

  /**
   * Filters virtual columns to only include ones referenced by the given {@link DimFilter}.
   * This removes virtual columns that were used by deletion rules that have been optimized away.
   *
   * @param filter         the reduced filter to check for column references
   * @param virtualColumns the original set of virtual columns
   * @return filtered VirtualColumns with only referenced columns, or null if none are referenced
   */
  @Nullable
  private VirtualColumns filterVirtualColumnsForFilter(
      @Nullable DimFilter filter,
      @Nullable VirtualColumns virtualColumns
  )
  {
    if (virtualColumns == null || filter == null) {
      return null;
    }

    // Get the set of columns required by the filter
    Set<String> requiredColumns = filter.getRequiredColumns();

    // Filter virtual columns to only include ones whose output name is required
    List<VirtualColumn> referencedColumns = new ArrayList<>();
    for (VirtualColumn vc : virtualColumns.getVirtualColumns()) {
      if (requiredColumns.contains(vc.getOutputName())) {
        referencedColumns.add(vc);
      }
    }

    // Return null if no virtual columns are referenced, otherwise create new VirtualColumns
    return referencedColumns.isEmpty() ? null : VirtualColumns.create(referencedColumns);
  }

  /**
   * Extracts the set of applied filters from a {@link CompactionState}.
   *
   * @param state the {@link CompactionState} to extract applied filters from
   * @return the set of applied filters, or null if transform spec or filter is null (indicating 0 applied filters)
   */
  @Nullable
  private static Set<DimFilter> extractAppliedFilters(CompactionState state)
  {
    if (state.getTransformSpec() == null) {
      return null;
    }

    DimFilter filter = state.getTransformSpec().getFilter();
    if (filter == null) {
      return null;
    }

    if (!(filter instanceof NotDimFilter)) {
      return Collections.emptySet();
    }

    DimFilter inner = ((NotDimFilter) filter).getField();

    if (inner instanceof OrDimFilter) {
      return new HashSet<>(((OrDimFilter) inner).getFields());
    } else {
      return Collections.singleton(inner);
    }
  }

  /**
   * Determines if we should optimize filter rules for this candidate.
   * Returns true only if the candidate has been compacted before and has a NotDimFilter.
   */
  private boolean shouldOptimizeFilterRules(
      CompactionCandidate candidate,
      DataSourceCompactionConfig config
  )
  {
    if (candidate.getCurrentStatus() == null) {
      return false;
    }

    if (candidate.getCurrentStatus().getReason().equals(CompactionStatus.NEVER_COMPACTED_REASON)) {
      return false;
    }

    if (config.getTransformSpec() == null) {
      return false;
    }

    DimFilter filter = config.getTransformSpec().getFilter();
    return filter instanceof NotDimFilter;
  }
}
