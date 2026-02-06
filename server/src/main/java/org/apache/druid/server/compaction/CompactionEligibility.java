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

import com.google.common.base.Strings;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.common.config.Configs;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.metadata.IndexingStateFingerprintMapper;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Describes the eligibility of an interval for compaction.
 */
public class CompactionEligibility
{
  public enum State
  {
    FULL_COMPACTION,
    INCREMENTAL_COMPACTION,
    NOT_ELIGIBLE,
    NOT_APPLICABLE
  }

  static class CompactionEligibilityBuilder
  {
    private State state;
    private CompactionStatistics compacted;
    private CompactionStatistics uncompacted;
    private List<DataSegment> uncompactedSegments;
    private String reason;

    CompactionEligibilityBuilder(State state, String reason)
    {
      this.state = state;
      this.reason = reason;
    }

    CompactionEligibilityBuilder compacted(CompactionStatistics compacted)
    {
      this.compacted = compacted;
      return this;
    }

    CompactionEligibilityBuilder uncompacted(CompactionStatistics uncompacted)
    {
      this.uncompacted = uncompacted;
      return this;
    }

    CompactionEligibilityBuilder uncompactedSegments(List<DataSegment> uncompactedSegments)
    {
      this.uncompactedSegments = uncompactedSegments;
      return this;
    }

    CompactionEligibility build()
    {
      return new CompactionEligibility(state, reason, compacted, uncompacted, uncompactedSegments);
    }
  }

  public static final CompactionEligibility NOT_APPLICABLE = builder(State.NOT_APPLICABLE, "").build();

  public static CompactionEligibility fail(String messageFormat, Object... args)
  {
    return builder(State.NOT_ELIGIBLE, StringUtils.format(messageFormat, args)).build();
  }

  private final State state;
  private final String reason;

  @Nullable
  private final CompactionStatistics compacted;
  @Nullable
  private final CompactionStatistics uncompacted;
  @Nullable
  private final List<DataSegment> uncompactedSegments;

  private CompactionEligibility(
      State state,
      String reason,
      @Nullable CompactionStatistics compacted,
      @Nullable CompactionStatistics uncompacted,
      @Nullable List<DataSegment> uncompactedSegments
  )
  {
    this.state = state;
    this.reason = reason;
    switch (state) {
      case NOT_APPLICABLE:
        break;
      case NOT_ELIGIBLE:
        InvalidInput.conditionalException(!Strings.isNullOrEmpty(reason), "must provide a reason");
        break;
      case FULL_COMPACTION:
      case INCREMENTAL_COMPACTION:
        InvalidInput.conditionalException(compacted != null, "must provide compacted stats");
        InvalidInput.conditionalException(uncompacted != null, "must provide uncompacted stats");
        InvalidInput.conditionalException(uncompactedSegments != null, "must provide uncompactedSegments");
        break;
      default:
        throw DruidException.defensive("unexpected eligibility state[%s]", state);
    }
    this.compacted = compacted;
    this.uncompacted = uncompacted;
    this.uncompactedSegments = uncompactedSegments;
  }

  static CompactionEligibilityBuilder builder(State state, String reason)
  {
    return new CompactionEligibilityBuilder(state, reason);
  }

  public State getState()
  {
    return state;
  }

  public String getReason()
  {
    return reason;
  }

  @Nullable
  public CompactionStatistics getUncompactedStats()
  {
    return uncompacted;
  }

  @Nullable
  public CompactionStatistics getCompactedStats()
  {
    return compacted;
  }

  @Nullable
  public List<DataSegment> getUncompactedSegments()
  {
    return uncompactedSegments;
  }

  public CompactionCandidate createCandidate(CompactionCandidate.ProposedCompaction proposedCompaction)
  {
    switch (state) {
      case NOT_APPLICABLE:
        return new CompactionCandidate(proposedCompaction, this, CompactionStatus.COMPLETE);
      case NOT_ELIGIBLE:
        return new CompactionCandidate(proposedCompaction, this, CompactionStatus.skipped(reason));
      case FULL_COMPACTION:
        return new CompactionCandidate(
            proposedCompaction,
            this,
            CompactionStatus.pending(reason)
        );
      case INCREMENTAL_COMPACTION:
        CompactionCandidate.ProposedCompaction newProposed = new CompactionCandidate.ProposedCompaction(
            uncompactedSegments,
            proposedCompaction.getUmbrellaInterval(),
            proposedCompaction.getCompactionInterval(),
            Math.toIntExact(uncompactedSegments.stream().map(DataSegment::getInterval).distinct().count())
        );
        return new CompactionCandidate(newProposed, this, CompactionStatus.pending(reason));
      default:
        throw DruidException.defensive("Unexpected eligibility state[%s]", state);
    }
  }

  /**
   * Evaluates a compaction candidate to determine its eligibility and compaction status.
   * <p>
   * This method performs a two-stage evaluation:
   * <ol>
   * <li>First, uses {@link Evaluator} to check if the candidate needs compaction
   *     based on the compaction config (e.g., checking segment granularity, partitions spec, etc.)</li>
   * <li>Then, applies the search policy to determine if this candidate should be compacted in the
   *     current run (e.g., checking minimum segment count, bytes, or other policy criteria)</li>
   * </ol>
   *
   * @param proposedCompaction the compaction candidate to evaluate
   * @param config             the compaction configuration for the datasource
   * @param searchPolicy       the policy that determines candidate ordering and eligibility
   * @param fingerprintMapper  mapper for indexing state fingerprints
   * @return a new {@link CompactionCandidate} with updated eligibility and status. For incremental
   * compaction, returns a candidate containing only the uncompacted segments.
   */
  public static CompactionEligibility evaluate(
      CompactionCandidate.ProposedCompaction proposedCompaction,
      DataSourceCompactionConfig config,
      CompactionCandidateSearchPolicy searchPolicy,
      IndexingStateFingerprintMapper fingerprintMapper
  )
  {
    // ideally we should let this class only decides CompactionEligibility, and the callsite should handle recreation of candidate.
    CompactionEligibility evaluatedCandidate = new Evaluator(proposedCompaction, config, fingerprintMapper).evaluate();
    switch (Objects.requireNonNull(evaluatedCandidate).getState()) {
      case NOT_APPLICABLE:
      case NOT_ELIGIBLE:
        return evaluatedCandidate;
      case FULL_COMPACTION: // evaluator has decided compaction is needed, policy needs to further check
        return searchPolicy.checkEligibilityForCompaction(proposedCompaction, evaluatedCandidate);
      case INCREMENTAL_COMPACTION: // evaluator cant decide when to perform an incremental compaction
      default:
        throw DruidException.defensive("Unexpected eligibility[%s]", evaluatedCandidate);
    }
  }

  @Override
  public boolean equals(Object object)
  {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    CompactionEligibility that = (CompactionEligibility) object;
    return state == that.state
           && Objects.equals(reason, that.reason)
           && Objects.equals(compacted, that.compacted)
           && Objects.equals(uncompacted, that.uncompacted)
           && Objects.equals(uncompactedSegments, that.uncompactedSegments);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(state, reason, compacted, uncompacted, uncompactedSegments);
  }

  @Override
  public String toString()
  {
    return "CompactionEligibility{"
           + "state=" + state
           + ", reason='" + reason + '\''
           + ", compacted=" + compacted
           + ", uncompacted=" + uncompacted
           + ", uncompactedSegments=" + uncompactedSegments
           + '}';
  }

  /**
   * List of checks performed to determine if compaction is already complete based on indexing state fingerprints.
   */
  static final List<Function<Evaluator, String>> FINGERPRINT_CHECKS = List.of(
      Evaluator::allFingerprintedCandidatesHaveExpectedFingerprint
  );

  /**
   * List of checks performed to determine if compaction is already complete.
   * <p>
   * The order of the checks must be honored while evaluating them.
   */
  static final List<Function<Evaluator, String>> CHECKS = Arrays.asList(
      Evaluator::partitionsSpecIsUpToDate,
      Evaluator::indexSpecIsUpToDate,
      Evaluator::segmentGranularityIsUpToDate,
      Evaluator::queryGranularityIsUpToDate,
      Evaluator::rollupIsUpToDate,
      Evaluator::dimensionsSpecIsUpToDate,
      Evaluator::metricsSpecIsUpToDate,
      Evaluator::transformSpecFilterIsUpToDate,
      Evaluator::projectionsAreUpToDate
  );

  /**
   * Evaluates checks to determine the compaction status of a
   * {@link CompactionCandidate}.
   */
  private static class Evaluator
  {
    private static final Logger log = new Logger(Evaluator.class);

    private final DataSourceCompactionConfig compactionConfig;
    private final CompactionCandidate.ProposedCompaction proposedCompaction;
    private final ClientCompactionTaskQueryTuningConfig tuningConfig;
    private final UserCompactionTaskGranularityConfig configuredGranularitySpec;

    private final List<DataSegment> fingerprintedSegments = new ArrayList<>();
    private final List<DataSegment> compactedSegments = new ArrayList<>();
    private final List<DataSegment> uncompactedSegments = new ArrayList<>();
    private final Map<CompactionState, List<DataSegment>> unknownStateToSegments = new HashMap<>();

    @Nullable
    private final IndexingStateFingerprintMapper fingerprintMapper;
    @Nullable
    private final String targetFingerprint;

    private Evaluator(
        CompactionCandidate.ProposedCompaction proposedCompaction,
        DataSourceCompactionConfig compactionConfig,
        @Nullable IndexingStateFingerprintMapper fingerprintMapper
    )
    {
      this.proposedCompaction = proposedCompaction;
      this.compactionConfig = compactionConfig;
      this.tuningConfig = ClientCompactionTaskQueryTuningConfig.from(compactionConfig);
      this.configuredGranularitySpec = compactionConfig.getGranularitySpec();
      this.fingerprintMapper = fingerprintMapper;
      if (fingerprintMapper == null) {
        targetFingerprint = null;
      } else {
        targetFingerprint = fingerprintMapper.generateFingerprint(
            compactionConfig.getDataSource(),
            compactionConfig.toCompactionState()
        );
      }
    }

    /**
     * Evaluates the compaction status of candidate segments through a multi-step process:
     * <ol>
     *   <li>Validates input bytes are within limits</li>
     *   <li>Categorizes segments by compaction state (fingerprinted, uncompacted, or unknown)</li>
     *   <li>Performs fingerprint-based validation if available (fast path)</li>
     *   <li>Runs detailed checks against unknown states via {@link CompactionEligibility#CHECKS}</li>
     * </ol>
     *
     * @return Pair of eligibility status and compaction status with reason for first failed check
     */
    private CompactionEligibility evaluate()
    {
      final String inputBytesCheck = inputBytesAreWithinLimit();
      if (inputBytesCheck != null) {
        return CompactionEligibility.fail(inputBytesCheck);
      }

      List<String> reasonsForCompaction = new ArrayList<>();
      String compactedOnceCheck = segmentsHaveBeenCompactedAtLeastOnce();
      if (compactedOnceCheck != null) {
        reasonsForCompaction.add(compactedOnceCheck);
      }

      if (fingerprintMapper != null && targetFingerprint != null) {
        // First try fingerprint-based evaluation (fast path)
        FINGERPRINT_CHECKS.stream()
                          .map(f -> f.apply(this))
                          .filter(Objects::nonNull)
                          .findFirst()
                          .ifPresent(reasonsForCompaction::add);

      }

      if (!unknownStateToSegments.isEmpty()) {
        // Run CHECKS against any states with uknown compaction status
        reasonsForCompaction.addAll(
            CHECKS.stream()
                  .map(f -> f.apply(this))
                  .filter(Objects::nonNull)
                  .collect(Collectors.toList())
        );

        // Any segments left in unknownStateToSegments passed all checks and are considered compacted
        compactedSegments.addAll(
            unknownStateToSegments
                .values()
                .stream()
                .flatMap(List::stream)
                .collect(Collectors.toList())
        );
      }

      if (reasonsForCompaction.isEmpty()) {
        return CompactionEligibility.NOT_APPLICABLE;
      } else {
        return builder(State.FULL_COMPACTION, reasonsForCompaction.get(0)).compacted(createStats(compactedSegments))
                                                                          .uncompacted(createStats(uncompactedSegments))
                                                                          .uncompactedSegments(uncompactedSegments)
                                                                          .build();
      }
    }

    /**
     * Evaluates the fingerprints of all fingerprinted candidate segments against the expected fingerprint.
     * <p>
     * If all fingerprinted segments have the expected fingerprint, the check can quickly pass as COMPLETE. However,
     * if any fingerprinted segment has a mismatched fingerprint, we need to investigate further by adding them to
     * {@link #unknownStateToSegments} where their indexing states will be analyzed.
     * </p>
     */
    private String allFingerprintedCandidatesHaveExpectedFingerprint()
    {
      Map<String, List<DataSegment>> mismatchedFingerprintToSegmentMap = new HashMap<>();
      for (DataSegment segment : fingerprintedSegments) {
        String fingerprint = segment.getIndexingStateFingerprint();
        if (fingerprint == null) {
          // Should not happen since we are iterating over fingerprintedSegments
        } else if (fingerprint.equals(targetFingerprint)) {
          compactedSegments.add(segment);
        } else {
          mismatchedFingerprintToSegmentMap
              .computeIfAbsent(fingerprint, k -> new ArrayList<>())
              .add(segment);
        }
      }

      if (mismatchedFingerprintToSegmentMap.isEmpty()) {
        // All fingerprinted segments have the expected fingerprint - compaction is complete
        return null;
      }

      if (fingerprintMapper == null) {
        // Cannot evaluate further without a fingerprint mapper
        uncompactedSegments.addAll(
            mismatchedFingerprintToSegmentMap.values()
                                             .stream()
                                             .flatMap(List::stream)
                                             .collect(Collectors.toList())
        );
        return "Segments have a mismatched fingerprint and no fingerprint mapper is available";
      }

      boolean fingerprintedSegmentWithoutCachedStateFound = false;

      for (Map.Entry<String, List<DataSegment>> e : mismatchedFingerprintToSegmentMap.entrySet()) {
        String fingerprint = e.getKey();
        CompactionState stateToValidate = fingerprintMapper.getStateForFingerprint(fingerprint).orElse(null);
        if (stateToValidate == null) {
          log.warn("No indexing state found for fingerprint[%s]", fingerprint);
          fingerprintedSegmentWithoutCachedStateFound = true;
          uncompactedSegments.addAll(e.getValue());
        } else {
          // Note that this does not mean we need compaction yet - we need to validate the state further to determine this
          unknownStateToSegments.compute(
              stateToValidate,
              (state, segments) -> {
                if (segments == null) {
                  segments = new ArrayList<>();
                }
                segments.addAll(e.getValue());
                return segments;
              }
          );
        }
      }

      if (fingerprintedSegmentWithoutCachedStateFound) {
        return "One or more fingerprinted segments do not have a cached indexing state";
      } else {
        return null;
      }
    }

    /**
     * Checks if all the segments have been compacted at least once and groups them into uncompacted, fingerprinted, or
     * non-fingerprinted.
     */
    private String segmentsHaveBeenCompactedAtLeastOnce()
    {
      for (DataSegment segment : proposedCompaction.getSegments()) {
        final String fingerprint = segment.getIndexingStateFingerprint();
        final CompactionState segmentState = segment.getLastCompactionState();
        if (fingerprint != null) {
          fingerprintedSegments.add(segment);
        } else if (segmentState == null) {
          uncompactedSegments.add(segment);
        } else {
          unknownStateToSegments.computeIfAbsent(segmentState, s -> new ArrayList<>()).add(segment);
        }
      }

      if (uncompactedSegments.isEmpty()) {
        return null;
      } else {
        return "not compacted yet";
      }
    }

    private String partitionsSpecIsUpToDate()
    {
      return evaluateForAllCompactionStates(this::partitionsSpecIsUpToDate);
    }

    private String indexSpecIsUpToDate()
    {
      return evaluateForAllCompactionStates(this::indexSpecIsUpToDate);
    }

    private String projectionsAreUpToDate()
    {
      return evaluateForAllCompactionStates(this::projectionsAreUpToDate);
    }

    private String segmentGranularityIsUpToDate()
    {
      return evaluateForAllCompactionStates(this::segmentGranularityIsUpToDate);
    }

    private String rollupIsUpToDate()
    {
      return evaluateForAllCompactionStates(this::rollupIsUpToDate);
    }

    private String queryGranularityIsUpToDate()
    {
      return evaluateForAllCompactionStates(this::queryGranularityIsUpToDate);
    }

    private String dimensionsSpecIsUpToDate()
    {
      return evaluateForAllCompactionStates(this::dimensionsSpecIsUpToDate);
    }

    private String metricsSpecIsUpToDate()
    {
      return evaluateForAllCompactionStates(this::metricsSpecIsUpToDate);
    }

    private String transformSpecFilterIsUpToDate()
    {
      return evaluateForAllCompactionStates(this::transformSpecFilterIsUpToDate);
    }

    private String partitionsSpecIsUpToDate(CompactionState lastCompactionState)
    {
      PartitionsSpec existingPartionsSpec = lastCompactionState.getPartitionsSpec();
      if (existingPartionsSpec instanceof DimensionRangePartitionsSpec) {
        existingPartionsSpec = getEffectiveRangePartitionsSpec((DimensionRangePartitionsSpec) existingPartionsSpec);
      } else if (existingPartionsSpec instanceof DynamicPartitionsSpec) {
        existingPartionsSpec = new DynamicPartitionsSpec(
            existingPartionsSpec.getMaxRowsPerSegment(),
            ((DynamicPartitionsSpec) existingPartionsSpec).getMaxTotalRowsOr(Long.MAX_VALUE)
        );
      }
      return completeIfNullOrEqual(
          "partitionsSpec",
          findPartitionsSpecFromConfig(tuningConfig),
          existingPartionsSpec,
          CompactionEligibility::asString
      );
    }

    private String indexSpecIsUpToDate(CompactionState lastCompactionState)
    {
      return completeIfNullOrEqual(
          "indexSpec",
          Configs.valueOrDefault(tuningConfig.getIndexSpec(), IndexSpec.getDefault()).getEffectiveSpec(),
          lastCompactionState.getIndexSpec().getEffectiveSpec(),
          String::valueOf
      );
    }

    private String projectionsAreUpToDate(CompactionState lastCompactionState)
    {
      return completeIfNullOrEqual(
          "projections",
          compactionConfig.getProjections(),
          lastCompactionState.getProjections(),
          String::valueOf
      );
    }

    @Nullable
    private String inputBytesAreWithinLimit()
    {
      final long inputSegmentSize = compactionConfig.getInputSegmentSizeBytes();
      if (proposedCompaction.getTotalBytes() > inputSegmentSize) {
        return StringUtils.format(
            "'inputSegmentSize' exceeded: Total segment size[%d] is larger than allowed inputSegmentSize[%d]",
            proposedCompaction.getTotalBytes(), inputSegmentSize
        );
      }
      return null;
    }

    private String segmentGranularityIsUpToDate(CompactionState lastCompactionState)
    {
      if (configuredGranularitySpec == null
          || configuredGranularitySpec.getSegmentGranularity() == null) {
        return null;
      }

      final Granularity configuredSegmentGranularity = configuredGranularitySpec.getSegmentGranularity();
      final UserCompactionTaskGranularityConfig existingGranularitySpec = getGranularitySpec(lastCompactionState);
      final Granularity existingSegmentGranularity
          = existingGranularitySpec == null ? null : existingGranularitySpec.getSegmentGranularity();

      if (configuredSegmentGranularity.equals(existingSegmentGranularity)) {
        return null;
      } else if (existingSegmentGranularity == null) {
        // Candidate segments were compacted without segment granularity specified
        // Check if the segments already have the desired segment granularity
        final List<DataSegment> segmentsForState = unknownStateToSegments.get(lastCompactionState);
        boolean needsCompaction = segmentsForState.stream().anyMatch(
            segment -> !configuredSegmentGranularity.isAligned(segment.getInterval())
        );
        if (needsCompaction) {
          return StringUtils.format(
              "segmentGranularity: segments do not align with target[%s]",
              CompactionEligibility.asString(configuredSegmentGranularity)
          );
        }
      } else {
        return configChanged(
            "segmentGranularity",
            configuredSegmentGranularity,
            existingSegmentGranularity,
            CompactionEligibility::asString
        );
      }

      return null;
    }

    private String rollupIsUpToDate(CompactionState lastCompactionState)
    {
      if (configuredGranularitySpec == null) {
        return null;
      } else {
        final UserCompactionTaskGranularityConfig existingGranularitySpec
            = getGranularitySpec(lastCompactionState);
        return completeIfNullOrEqual(
            "rollup",
            configuredGranularitySpec.isRollup(),
            existingGranularitySpec == null ? null : existingGranularitySpec.isRollup(),
            String::valueOf
        );
      }
    }

    private String queryGranularityIsUpToDate(CompactionState lastCompactionState)
    {
      if (configuredGranularitySpec == null) {
        return null;
      } else {
        final UserCompactionTaskGranularityConfig existingGranularitySpec
            = getGranularitySpec(lastCompactionState);
        return completeIfNullOrEqual(
            "queryGranularity",
            configuredGranularitySpec.getQueryGranularity(),
            existingGranularitySpec == null ? null : existingGranularitySpec.getQueryGranularity(),
            CompactionEligibility::asString
        );
      }
    }

    /**
     * Removes partition dimensions before comparison, since they are placed in front of the sort order --
     * which can create a mismatch between expected and actual order of dimensions. Partition dimensions are separately
     * covered in {@link Evaluator#partitionsSpecIsUpToDate()} check.
     */
    private String dimensionsSpecIsUpToDate(CompactionState lastCompactionState)
    {
      if (compactionConfig.getDimensionsSpec() == null) {
        return null;
      } else {
        List<DimensionSchema> existingDimensions = getNonPartitioningDimensions(
            lastCompactionState.getDimensionsSpec() == null
            ? null
            : lastCompactionState.getDimensionsSpec().getDimensions(),
            lastCompactionState.getPartitionsSpec(),
            lastCompactionState.getIndexSpec()
        );
        List<DimensionSchema> configuredDimensions = getNonPartitioningDimensions(
            compactionConfig.getDimensionsSpec().getDimensions(),
            compactionConfig.getTuningConfig() == null ? null : compactionConfig.getTuningConfig().getPartitionsSpec(),
            compactionConfig.getTuningConfig() == null
            ? IndexSpec.getDefault()
            : compactionConfig.getTuningConfig().getIndexSpec()
        );
        return completeIfNullOrEqual(
            "dimensionsSpec",
            configuredDimensions,
            existingDimensions,
            String::valueOf
        );
      }
    }

    private String metricsSpecIsUpToDate(CompactionState lastCompactionState)
    {
      final AggregatorFactory[] configuredMetricsSpec = compactionConfig.getMetricsSpec();
      if (ArrayUtils.isEmpty(configuredMetricsSpec)) {
        return null;
      }

      final List<AggregatorFactory> metricSpecList = lastCompactionState.getMetricsSpec();
      final AggregatorFactory[] existingMetricsSpec
          = CollectionUtils.isNullOrEmpty(metricSpecList)
            ? null : metricSpecList.toArray(new AggregatorFactory[0]);

      if (existingMetricsSpec == null || !Arrays.deepEquals(configuredMetricsSpec, existingMetricsSpec)) {
        return configChanged(
            "metricsSpec",
            configuredMetricsSpec,
            existingMetricsSpec,
            Arrays::toString
        );
      } else {
        return null;
      }
    }

    private String transformSpecFilterIsUpToDate(CompactionState lastCompactionState)
    {
      if (compactionConfig.getTransformSpec() == null) {
        return null;
      }

      CompactionTransformSpec existingTransformSpec = lastCompactionState.getTransformSpec();
      return completeIfNullOrEqual(
          "transformSpec filter",
          compactionConfig.getTransformSpec().getFilter(),
          existingTransformSpec == null ? null : existingTransformSpec.getFilter(),
          String::valueOf
      );
    }

    /**
     * Evaluates the given check for each entry in the {@link #unknownStateToSegments}.
     * If any entry fails the given check by returning a status which is not
     * COMPLETE, all the segments with that state are moved to {@link #uncompactedSegments}.
     *
     * @return The first status which is not COMPLETE.
     */
    private String evaluateForAllCompactionStates(Function<CompactionState, String> check)
    {
      String firstIncomplete = null;
      for (CompactionState state : List.copyOf(unknownStateToSegments.keySet())) {
        final String eligibleReason = check.apply(state);
        if (eligibleReason != null) {
          uncompactedSegments.addAll(unknownStateToSegments.remove(state));
          if (firstIncomplete == null) {
            firstIncomplete = eligibleReason;
          }
        }
      }

      return firstIncomplete;
    }

    private static UserCompactionTaskGranularityConfig getGranularitySpec(
        CompactionState compactionState
    )
    {
      return UserCompactionTaskGranularityConfig.from(compactionState.getGranularitySpec());
    }

    private static CompactionStatistics createStats(List<DataSegment> segments)
    {
      final Set<Interval> segmentIntervals =
          segments.stream().map(DataSegment::getInterval).collect(Collectors.toSet());
      final long totalBytes = segments.stream().mapToLong(DataSegment::getSize).sum();
      return CompactionStatistics.create(totalBytes, segments.size(), segmentIntervals.size());
    }
  }


  /**
   * Computes compaction status for the given field. The status is assumed to be
   * COMPLETE (i.e. no further compaction is required) if the configured value
   * of the field is null or equal to the current value.
   */
  private static <T> String completeIfNullOrEqual(
      String field,
      T configured,
      T current,
      Function<T, String> stringFunction
  )
  {
    if (configured == null || configured.equals(current)) {
      return null;
    } else {
      return configChanged(field, configured, current, stringFunction);
    }
  }

  private static <T> String configChanged(
      String field,
      T target,
      T current,
      Function<T, String> stringFunction
  )
  {
    return StringUtils.format(
        "'%s' mismatch: required[%s], current[%s]",
        field,
        target == null ? null : stringFunction.apply(target),
        current == null ? null : stringFunction.apply(current)
    );
  }

  private static String asString(Granularity granularity)
  {
    if (granularity == null) {
      return null;
    }
    for (GranularityType type : GranularityType.values()) {
      if (type.getDefaultGranularity().equals(granularity)) {
        return type.toString();
      }
    }
    return granularity.toString();
  }

  private static String asString(PartitionsSpec partitionsSpec)
  {
    if (partitionsSpec instanceof DimensionRangePartitionsSpec) {
      DimensionRangePartitionsSpec rangeSpec = (DimensionRangePartitionsSpec) partitionsSpec;
      return StringUtils.format(
          "'range' on %s with %,d rows",
          rangeSpec.getPartitionDimensions(), rangeSpec.getTargetRowsPerSegment()
      );
    } else if (partitionsSpec instanceof HashedPartitionsSpec) {
      HashedPartitionsSpec hashedSpec = (HashedPartitionsSpec) partitionsSpec;
      return StringUtils.format(
          "'hashed' on %s with %,d rows",
          hashedSpec.getPartitionDimensions(), hashedSpec.getTargetRowsPerSegment()
      );
    } else if (partitionsSpec instanceof DynamicPartitionsSpec) {
      DynamicPartitionsSpec dynamicSpec = (DynamicPartitionsSpec) partitionsSpec;
      return StringUtils.format(
          "'dynamic' with %,d rows",
          dynamicSpec.getMaxRowsPerSegment()
      );
    } else {
      return partitionsSpec.toString();
    }
  }

  @Nullable
  public static PartitionsSpec findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig tuningConfig)
  {
    final PartitionsSpec partitionsSpecFromTuningConfig = tuningConfig.getPartitionsSpec();
    if (partitionsSpecFromTuningConfig == null) {
      final Long maxTotalRows = tuningConfig.getMaxTotalRows();
      final Integer maxRowsPerSegment = tuningConfig.getMaxRowsPerSegment();

      if (maxTotalRows == null && maxRowsPerSegment == null) {
        // If not specified, return null so that partitionsSpec is not compared
        return null;
      } else {
        return new DynamicPartitionsSpec(maxRowsPerSegment, maxTotalRows);
      }
    } else if (partitionsSpecFromTuningConfig instanceof DynamicPartitionsSpec) {
      return new DynamicPartitionsSpec(
          partitionsSpecFromTuningConfig.getMaxRowsPerSegment(),
          ((DynamicPartitionsSpec) partitionsSpecFromTuningConfig).getMaxTotalRowsOr(Long.MAX_VALUE)
      );
    } else if (partitionsSpecFromTuningConfig instanceof DimensionRangePartitionsSpec) {
      return getEffectiveRangePartitionsSpec((DimensionRangePartitionsSpec) partitionsSpecFromTuningConfig);
    } else {
      return partitionsSpecFromTuningConfig;
    }
  }

  @Nullable
  private static List<DimensionSchema> getNonPartitioningDimensions(
      @Nullable final List<DimensionSchema> dimensionSchemas,
      @Nullable final PartitionsSpec partitionsSpec,
      @Nullable final IndexSpec indexSpec
  )
  {
    final IndexSpec effectiveIndexSpec = (indexSpec == null ? IndexSpec.getDefault() : indexSpec).getEffectiveSpec();
    if (dimensionSchemas == null || !(partitionsSpec instanceof DimensionRangePartitionsSpec)) {
      if (dimensionSchemas != null) {
        return dimensionSchemas.stream()
                               .map(dim -> dim.getEffectiveSchema(effectiveIndexSpec))
                               .collect(Collectors.toList());
      }
      return null;
    }

    final List<String> partitionsDimensions = ((DimensionRangePartitionsSpec) partitionsSpec).getPartitionDimensions();
    return dimensionSchemas.stream()
                           .filter(dim -> !partitionsDimensions.contains(dim.getName()))
                           .map(dim -> dim.getEffectiveSchema(effectiveIndexSpec))
                           .collect(Collectors.toList());
  }

  /**
   * Converts to have only the effective maxRowsPerSegment to avoid false positives when targetRowsPerSegment is set but
   * effectively translates to the same maxRowsPerSegment.
   */
  static DimensionRangePartitionsSpec getEffectiveRangePartitionsSpec(DimensionRangePartitionsSpec partitionsSpec)
  {
    return new DimensionRangePartitionsSpec(
        null,
        partitionsSpec.getMaxRowsPerSegment(),
        partitionsSpec.getPartitionDimensions(),
        partitionsSpec.isAssumeGrouped()
    );
  }
}
