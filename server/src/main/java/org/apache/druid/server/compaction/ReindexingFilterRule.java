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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.segment.metadata.IndexingStateFingerprintMapper;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A compaction filter rule that specifies rows to remove from segments older than a specified period.
 * <p>
 * The filter defines rows to REMOVE from compacted segments. For example, a filter
 * {@code selector(isRobot=true)} means "remove rows where isRobot=true". The compaction framework
 * automatically wraps these filters in NOT logic during processing.
 * <p>
 * Rules are evaluated at compaction time based on segment age. A rule with period P90D will apply
 * to any segment where the segment's end time is before ("now" - 90 days).
 * <p>
 * Multiple rules can apply to the same segment. When multiple rules apply, they are combined as
 * NOT(A OR B OR C) for optimal bitmap performance, which is equivalent to NOT A AND NOT B AND NOT C
 * but uses fewer operations.
 * <p>
 * Example usage:
 * <pre>{@code
 * {
 *   "id": "remove-robots-90d",
 *   "period": "P90D",
 *   "filter": {
 *     "type": "selector",
 *     "dimension": "isRobot",
 *     "value": "true"
 *   },
 *   "description": "Remove robot traffic from segments older than 90 days"
 * }
 * }</pre>
 */
public class ReindexingFilterRule extends AbstractReindexingRule
{
  private static final Logger LOG = new Logger(ReindexingFilterRule.class);

  private final DimFilter filter;

  @JsonCreator
  public ReindexingFilterRule(
      @JsonProperty("id") @Nonnull String id,
      @JsonProperty("description") @Nullable String description,
      @JsonProperty("period") @Nonnull Period period,
      @JsonProperty("filter") @Nonnull DimFilter filter
  )
  {
    super(id, description, period);
    this.filter = Objects.requireNonNull(filter, "filter cannot be null");
  }

  @Override
  public boolean isAdditive()
  {
    return true;
  }

  @JsonProperty
  public DimFilter getFilter()
  {
    return filter;
  }

  /**
   * Computes the required set of filter rules to be applied for the given compaction candidate.
   * <p>
   * We only want to apply the rules that have not yet been applied to all segments in the candidate. This reduces
   * the amount of work the compaction task needs to do and avoids re-applying filters that have already been applied.
   * </p>
   *
   * @param candidateSegments the compaction candidate
   * @param expectedFilter the expected filter (as a NotDimFilter wrapping an OrDimFilter)
   * @param fingerprintMapper the fingerprint mapper to retrieve applied filters from segment fingerprints
   * @return the set of unapplied filter rules wrapped in a NotDimFilter, or null if all rules have been applied
   */
  @Nullable
  public static NotDimFilter computeRequiredSetOfFilterRulesForCandidate(
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

    // Collect unique fingerprints
    Set<String> uniqueFingerprints = candidateSegments.getSegments().stream()
                                                      .map(DataSegment::getIndexingStateFingerprint)
                                                      .filter(Objects::nonNull)
                                                      .collect(Collectors.toSet());

    if (uniqueFingerprints.isEmpty()) {
      // no fingerprints means that no candidate segments have transforms to compare against. Return all filters eagerly.
      return expectedFilter;
    }

    // Accumulate filters that haven't been applied across all fingerprints
    Set<DimFilter> unappliedRules = new HashSet<>();

    for (String fingerprint : uniqueFingerprints) {
      CompactionState state = fingerprintMapper.getStateForFingerprint(fingerprint).orElse(null);

      if (state == null) {
        // Safety: if state is missing, return all filters eagerly since we can't determine applied filters
        return expectedFilter;
      }

      // Extract applied filters from the CompactionState into a Set
      Set<DimFilter> appliedFilters = extractAppliedFilters(state);

      // If transform spec or filter for the CompactionState is null, return all expected filters eagerly
      if (appliedFilters == null) {
        return expectedFilter;
      }

      // Check which expected filters are NOT in the applied set and add them to unappliedRules
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

    // If all filters were applied, return null
    if (unappliedRules.isEmpty()) {
      return null;
    }

    // Return the delta as NOT(OR(unapplied filters))
    return new NotDimFilter(new OrDimFilter(new ArrayList<>(unappliedRules)));
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
}
