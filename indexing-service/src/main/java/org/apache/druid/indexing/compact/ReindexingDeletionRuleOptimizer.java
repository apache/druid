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
 * Optimization utilities for applying deletion rules during reindexing.
 * <p>
 * When reindexing with deletion rules, it is possible that candidate segments have already applied
 * some or all of the deletion rules in previous reindexing runs. Reapplying such rules would
 * be wasteful and redundant. This class provides functionality to optimize the set of rules to be
 * applied by any given reindexing task.
 * <p>
 * Uses a subtractive approach for virtual columns: starts with ALL virtual columns from the transform spec
 * (including both deletion and partitioning VCs), identifies which VCs belong to pruned deletion rules,
 * and removes only those. This preserves partitioning VCs and VCs from unapplied deletion rules.
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

    NotDimFilter originalFilter = (NotDimFilter) config.getTransformSpec().getFilter();

    NotDimFilter reducedFilter = computeRequiredSetOfFilterRulesForCandidate(
        candidate,
        originalFilter,
        params.getFingerprintMapper()
    );

    // Subtractive VC tracking: remove only VCs associated with pruned deletion rules
    VirtualColumns reducedVirtualColumns = subtractPrunedDeletionVirtualColumns(
        originalFilter,
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
   * Subtractively removes virtual columns that were associated with pruned (already-applied) deletion rules.
   * <p>
   * Algorithm:
   * <ol>
   *   <li>Start with ALL virtual columns from the original transform spec</li>
   *   <li>Determine which deletion filter sub-clauses were pruned (present in original but not in reduced)</li>
   *   <li>Identify columns required by the pruned filters</li>
   *   <li>Remove only those virtual columns whose output names are required exclusively by pruned filters</li>
   *   <li>All remaining VCs (from unapplied deletion rules + from partitioning) are preserved</li>
   * </ol>
   *
   * @param originalFilter the original NOT(OR(...)) filter before optimization
   * @param reducedFilter  the reduced filter after optimization, or null if all rules were applied
   * @param allVirtualColumns all virtual columns from the original transform spec
   * @return the filtered set of virtual columns, or null if none remain
   */
  @Nullable
  private VirtualColumns subtractPrunedDeletionVirtualColumns(
      NotDimFilter originalFilter,
      @Nullable NotDimFilter reducedFilter,
      @Nullable VirtualColumns allVirtualColumns
  )
  {
    if (allVirtualColumns == null) {
      return null;
    }

    // Determine which columns are required by pruned (already-applied) deletion rules
    Set<String> prunedRequiredColumns = reducedFilter == null
        ? originalFilter.getRequiredColumns()
        : computePrunedFilterRequiredColumns(originalFilter, reducedFilter);

    if (prunedRequiredColumns.isEmpty()) {
      // Nothing was pruned, return all VCs unchanged
      return allVirtualColumns;
    }

    // Columns still needed by the remaining filter (empty if all rules were applied)
    Set<String> retainedRequiredColumns = reducedFilter == null
        ? Collections.emptySet()
        : reducedFilter.getRequiredColumns();

    // Remove VCs that are required ONLY by pruned filters (not by reduced filter or partitioning)
    List<VirtualColumn> remaining = new ArrayList<>();
    for (VirtualColumn vc : allVirtualColumns.getVirtualColumns()) {
      String name = vc.getOutputName();
      if (prunedRequiredColumns.contains(name) && !retainedRequiredColumns.contains(name)) {
        // This VC was only needed by pruned deletion rules, remove it
        continue;
      }
      remaining.add(vc);
    }

    return remaining.isEmpty() ? null : VirtualColumns.create(remaining);
  }

  /**
   * Computes the set of columns required by deletion filter sub-clauses that were pruned
   * (present in original but not in reduced).
   */
  private Set<String> computePrunedFilterRequiredColumns(
      NotDimFilter originalFilter,
      NotDimFilter reducedFilter
  )
  {
    List<DimFilter> originalSubClauses = extractSubClauses(originalFilter);
    Set<DimFilter> reducedSubClauses = new HashSet<>(extractSubClauses(reducedFilter));

    Set<String> prunedColumns = new HashSet<>();
    for (DimFilter clause : originalSubClauses) {
      if (!reducedSubClauses.contains(clause)) {
        prunedColumns.addAll(clause.getRequiredColumns());
      }
    }
    return prunedColumns;
  }

  /**
   * Extracts the sub-clauses from a NOT(OR(...)) filter structure.
   */
  private List<DimFilter> extractSubClauses(NotDimFilter filter)
  {
    DimFilter inner = filter.getField();
    if (inner instanceof OrDimFilter) {
      return ((OrDimFilter) inner).getFields();
    } else {
      return Collections.singletonList(inner);
    }
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
