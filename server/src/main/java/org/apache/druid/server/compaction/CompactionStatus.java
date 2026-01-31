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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.common.config.Configs;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.Pair;
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
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents the status of compaction for a given {@link CompactionCandidate}.
 */
public class CompactionStatus
{
  private static final Logger log = new Logger(CompactionStatus.class);

  private static final CompactionStatus COMPLETE = new CompactionStatus(State.COMPLETE, null, null, null);

  public enum State
  {
    COMPLETE, PENDING, RUNNING, SKIPPED
  }

  /**
   * List of checks performed to determine if compaction is already complete based on indexing state fingerprints.
   */
  private static final List<Function<Evaluator, CompactionStatus>> FINGERPRINT_CHECKS = List.of(
      Evaluator::allFingerprintedCandidatesHaveExpectedFingerprint
  );

  /**
   * List of checks performed to determine if compaction is already complete.
   * <p>
   * The order of the checks must be honored while evaluating them.
   */
  private static final List<Function<Evaluator, CompactionStatus>> CHECKS = Arrays.asList(
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

  private final State state;
  private final String reason;
  private final CompactionStatistics compactedStats;
  private final CompactionStatistics uncompactedStats;

  private CompactionStatus(
      State state,
      String reason,
      CompactionStatistics compactedStats,
      CompactionStatistics uncompactedStats
  )
  {
    this.state = state;
    this.reason = reason;
    this.compactedStats = compactedStats;
    this.uncompactedStats = uncompactedStats;
  }

  public boolean isComplete()
  {
    return state == State.COMPLETE;
  }

  public boolean isSkipped()
  {
    return state == State.SKIPPED;
  }

  public String getReason()
  {
    return reason;
  }

  public State getState()
  {
    return state;
  }

  public CompactionStatistics getCompactedStats()
  {
    return compactedStats;
  }

  public CompactionStatistics getUncompactedStats()
  {
    return uncompactedStats;
  }

  @Override
  public String toString()
  {
    return "CompactionStatus{" +
           "state=" + state +
           ", reason=" + reason +
           ", compactedStats=" + compactedStats +
           ", uncompactedStats=" + uncompactedStats +
           '}';
  }

  public static CompactionStatus pending(String reasonFormat, Object... args)
  {
    return new CompactionStatus(State.PENDING, StringUtils.format(reasonFormat, args), null, null);
  }

  public static CompactionStatus pending(
      CompactionStatistics compactedStats,
      CompactionStatistics uncompactedStats,
      String reasonFormat,
      Object... args
  )
  {
    return new CompactionStatus(
        State.PENDING,
        StringUtils.format(reasonFormat, args),
        compactedStats,
        uncompactedStats
    );
  }

  /**
   * Computes compaction status for the given field. The status is assumed to be
   * COMPLETE (i.e. no further compaction is required) if the configured value
   * of the field is null or equal to the current value.
   */
  private static <T> CompactionStatus completeIfNullOrEqual(
      String field,
      T configured,
      T current,
      Function<T, String> stringFunction
  )
  {
    if (configured == null || configured.equals(current)) {
      return COMPLETE;
    } else {
      return configChanged(field, configured, current, stringFunction);
    }
  }

  private static <T> CompactionStatus configChanged(
      String field,
      T target,
      T current,
      Function<T, String> stringFunction
  )
  {
    return CompactionStatus.pending(
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

  public static CompactionStatus skipped(String reasonFormat, Object... args)
  {
    return new CompactionStatus(State.SKIPPED, StringUtils.format(reasonFormat, args), null, null);
  }

  public static CompactionStatus running(String message)
  {
    return new CompactionStatus(State.RUNNING, message, null, null);
  }

  /**
   * Determines the CompactionStatus of the given candidate segments by evaluating
   * the {@link #CHECKS} one by one. If any check returns an incomplete status,
   * further checks are still performed to determine the number of uncompacted
   * segments but only the first incomplete status is returned.
   */
  static CompactionStatus compute(
      CompactionCandidate candidateSegments,
      DataSourceCompactionConfig config,
      @Nullable IndexingStateFingerprintMapper fingerprintMapper
  )
  {
    final CompactionState expectedState = config.toCompactionState();
    String expectedFingerprint;
    if (fingerprintMapper == null) {
      expectedFingerprint = null;
    } else {
      expectedFingerprint = fingerprintMapper.generateFingerprint(
          config.getDataSource(),
          expectedState
      );
    }
    return new Evaluator(candidateSegments, config, expectedFingerprint, fingerprintMapper).evaluate().rhs;
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

  /**
   * Evaluates {@link #CHECKS} to determine the compaction status of a
   * {@link CompactionCandidate}.
   */
  static class Evaluator
  {
    private final DataSourceCompactionConfig compactionConfig;
    private final CompactionCandidate candidateSegments;
    private final ClientCompactionTaskQueryTuningConfig tuningConfig;
    private final UserCompactionTaskGranularityConfig configuredGranularitySpec;

    private final List<DataSegment> fingerprintedSegments = new ArrayList<>();
    private final List<DataSegment> compactedSegments = new ArrayList<>();
    final List<DataSegment> uncompactedSegments = new ArrayList<>();
    private final Map<CompactionState, List<DataSegment>> unknownStateToSegments = new HashMap<>();

    @Nullable
    private final String targetFingerprint;
    private final IndexingStateFingerprintMapper fingerprintMapper;

    Evaluator(
        CompactionCandidate candidateSegments,
        DataSourceCompactionConfig compactionConfig,
        @Nullable String targetFingerprint,
        @Nullable IndexingStateFingerprintMapper fingerprintMapper
    )
    {
      this.candidateSegments = candidateSegments;
      this.compactionConfig = compactionConfig;
      this.tuningConfig = ClientCompactionTaskQueryTuningConfig.from(compactionConfig);
      this.configuredGranularitySpec = compactionConfig.getGranularitySpec();
      this.targetFingerprint = targetFingerprint;
      this.fingerprintMapper = fingerprintMapper;
    }

    Pair<CompactionCandidateSearchPolicy.Eligibility, CompactionStatus> evaluate()
    {
      final CompactionCandidateSearchPolicy.Eligibility inputBytesCheck = inputBytesAreWithinLimit();
      if (inputBytesCheck != null) {
        return Pair.of(inputBytesCheck, CompactionStatus.skipped(inputBytesCheck.getReason()));
      }

      List<String> reasonsForCompaction = new ArrayList<>();
      CompactionStatus compactedOnceCheck = segmentsHaveBeenCompactedAtLeastOnce();
      if (!compactedOnceCheck.isComplete()) {
        reasonsForCompaction.add(compactedOnceCheck.getReason());
      }

      if (fingerprintMapper != null && targetFingerprint != null) {
        // First try fingerprint-based evaluation (fast path)
        CompactionStatus fingerprintStatus = FINGERPRINT_CHECKS.stream()
                                                               .map(f -> f.apply(this))
                                                               .filter(status -> !status.isComplete())
                                                               .findFirst().orElse(COMPLETE);

        if (!fingerprintStatus.isComplete()) {
          reasonsForCompaction.add(fingerprintStatus.getReason());
        }
      }

      if (!unknownStateToSegments.isEmpty()) {
        // Run CHECKS against any states with uknown compaction status
        reasonsForCompaction.addAll(
            CHECKS.stream()
                  .map(f -> f.apply(this))
                  .filter(status -> !status.isComplete())
                  .map(CompactionStatus::getReason)
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
        return Pair.of(
            CompactionCandidateSearchPolicy.Eligibility.fail("All checks are passed, no reason to compact"),
            CompactionStatus.COMPLETE
        );
      } else {
        return Pair.of(
            CompactionCandidateSearchPolicy.Eligibility.FULL_COMPACTION_OK,
            CompactionStatus.pending(
                createStats(compactedSegments),
                createStats(uncompactedSegments),
                reasonsForCompaction.get(0)
            )
        );
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
    private CompactionStatus allFingerprintedCandidatesHaveExpectedFingerprint()
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
        return COMPLETE;
      }

      if (fingerprintMapper == null) {
        // Cannot evaluate further without a fingerprint mapper
        uncompactedSegments.addAll(
            mismatchedFingerprintToSegmentMap.values()
                                             .stream()
                                             .flatMap(List::stream)
                                             .collect(Collectors.toList())
        );
        return CompactionStatus.pending("Segments have a mismatched fingerprint and no fingerprint mapper is available");
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
        return CompactionStatus.pending("One or more fingerprinted segments do not have a cached indexing state");
      } else {
        return COMPLETE;
      }
    }

    /**
     * Checks if all the segments have been compacted at least once and groups them into uncompacted, fingerprinted, or
     * non-fingerprinted.
     */
    private CompactionStatus segmentsHaveBeenCompactedAtLeastOnce()
    {
      for (DataSegment segment : candidateSegments.getSegments()) {
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
        return COMPLETE;
      } else {
        return CompactionStatus.pending("not compacted yet");
      }
    }

    private CompactionStatus partitionsSpecIsUpToDate()
    {
      return evaluateForAllCompactionStates(this::partitionsSpecIsUpToDate);
    }

    private CompactionStatus indexSpecIsUpToDate()
    {
      return evaluateForAllCompactionStates(this::indexSpecIsUpToDate);
    }

    private CompactionStatus projectionsAreUpToDate()
    {
      return evaluateForAllCompactionStates(this::projectionsAreUpToDate);
    }

    private CompactionStatus segmentGranularityIsUpToDate()
    {
      return evaluateForAllCompactionStates(this::segmentGranularityIsUpToDate);
    }

    private CompactionStatus rollupIsUpToDate()
    {
      return evaluateForAllCompactionStates(this::rollupIsUpToDate);
    }

    private CompactionStatus queryGranularityIsUpToDate()
    {
      return evaluateForAllCompactionStates(this::queryGranularityIsUpToDate);
    }

    private CompactionStatus dimensionsSpecIsUpToDate()
    {
      return evaluateForAllCompactionStates(this::dimensionsSpecIsUpToDate);
    }

    private CompactionStatus metricsSpecIsUpToDate()
    {
      return evaluateForAllCompactionStates(this::metricsSpecIsUpToDate);
    }

    private CompactionStatus transformSpecFilterIsUpToDate()
    {
      return evaluateForAllCompactionStates(this::transformSpecFilterIsUpToDate);
    }

    private CompactionStatus partitionsSpecIsUpToDate(CompactionState lastCompactionState)
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
      return CompactionStatus.completeIfNullOrEqual(
          "partitionsSpec",
          findPartitionsSpecFromConfig(tuningConfig),
          existingPartionsSpec,
          CompactionStatus::asString
      );
    }

    private CompactionStatus indexSpecIsUpToDate(CompactionState lastCompactionState)
    {
      return CompactionStatus.completeIfNullOrEqual(
          "indexSpec",
          Configs.valueOrDefault(tuningConfig.getIndexSpec(), IndexSpec.getDefault()).getEffectiveSpec(),
          lastCompactionState.getIndexSpec().getEffectiveSpec(),
          String::valueOf
      );
    }

    private CompactionStatus projectionsAreUpToDate(CompactionState lastCompactionState)
    {
      return CompactionStatus.completeIfNullOrEqual(
          "projections",
          compactionConfig.getProjections(),
          lastCompactionState.getProjections(),
          String::valueOf
      );
    }

    @Nullable
    private CompactionCandidateSearchPolicy.Eligibility inputBytesAreWithinLimit()
    {
      final long inputSegmentSize = compactionConfig.getInputSegmentSizeBytes();
      if (candidateSegments.getTotalBytes() > inputSegmentSize) {
        return CompactionCandidateSearchPolicy.Eligibility.fail(
            "'inputSegmentSize' exceeded: Total segment size[%d] is larger than allowed inputSegmentSize[%d]",
            candidateSegments.getTotalBytes(), inputSegmentSize
        );
      }
      return null;
    }

    private CompactionStatus segmentGranularityIsUpToDate(CompactionState lastCompactionState)
    {
      if (configuredGranularitySpec == null
          || configuredGranularitySpec.getSegmentGranularity() == null) {
        return COMPLETE;
      }

      final Granularity configuredSegmentGranularity = configuredGranularitySpec.getSegmentGranularity();
      final UserCompactionTaskGranularityConfig existingGranularitySpec = getGranularitySpec(lastCompactionState);
      final Granularity existingSegmentGranularity
          = existingGranularitySpec == null ? null : existingGranularitySpec.getSegmentGranularity();

      if (configuredSegmentGranularity.equals(existingSegmentGranularity)) {
        return COMPLETE;
      } else if (existingSegmentGranularity == null) {
        // Candidate segments were compacted without segment granularity specified
        // Check if the segments already have the desired segment granularity
        final List<DataSegment> segmentsForState = unknownStateToSegments.get(lastCompactionState);
        boolean needsCompaction = segmentsForState.stream().anyMatch(
            segment -> !configuredSegmentGranularity.isAligned(segment.getInterval())
        );
        if (needsCompaction) {
          return CompactionStatus.pending(
              "segmentGranularity: segments do not align with target[%s]",
              asString(configuredSegmentGranularity)
          );
        }
      } else {
        return CompactionStatus.configChanged(
            "segmentGranularity",
            configuredSegmentGranularity,
            existingSegmentGranularity,
            CompactionStatus::asString
        );
      }

      return COMPLETE;
    }

    private CompactionStatus rollupIsUpToDate(CompactionState lastCompactionState)
    {
      if (configuredGranularitySpec == null) {
        return COMPLETE;
      } else {
        final UserCompactionTaskGranularityConfig existingGranularitySpec
            = getGranularitySpec(lastCompactionState);
        return CompactionStatus.completeIfNullOrEqual(
            "rollup",
            configuredGranularitySpec.isRollup(),
            existingGranularitySpec == null ? null : existingGranularitySpec.isRollup(),
            String::valueOf
        );
      }
    }

    private CompactionStatus queryGranularityIsUpToDate(CompactionState lastCompactionState)
    {
      if (configuredGranularitySpec == null) {
        return COMPLETE;
      } else {
        final UserCompactionTaskGranularityConfig existingGranularitySpec
            = getGranularitySpec(lastCompactionState);
        return CompactionStatus.completeIfNullOrEqual(
            "queryGranularity",
            configuredGranularitySpec.getQueryGranularity(),
            existingGranularitySpec == null ? null : existingGranularitySpec.getQueryGranularity(),
            CompactionStatus::asString
        );
      }
    }

    /**
     * Removes partition dimensions before comparison, since they are placed in front of the sort order --
     * which can create a mismatch between expected and actual order of dimensions. Partition dimensions are separately
     * covered in {@link Evaluator#partitionsSpecIsUpToDate()} check.
     */
    private CompactionStatus dimensionsSpecIsUpToDate(CompactionState lastCompactionState)
    {
      if (compactionConfig.getDimensionsSpec() == null) {
        return COMPLETE;
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
        return CompactionStatus.completeIfNullOrEqual(
            "dimensionsSpec",
            configuredDimensions,
            existingDimensions,
            String::valueOf
        );
      }
    }

    private CompactionStatus metricsSpecIsUpToDate(CompactionState lastCompactionState)
    {
      final AggregatorFactory[] configuredMetricsSpec = compactionConfig.getMetricsSpec();
      if (ArrayUtils.isEmpty(configuredMetricsSpec)) {
        return COMPLETE;
      }

      final List<AggregatorFactory> metricSpecList = lastCompactionState.getMetricsSpec();
      final AggregatorFactory[] existingMetricsSpec
          = CollectionUtils.isNullOrEmpty(metricSpecList)
            ? null : metricSpecList.toArray(new AggregatorFactory[0]);

      if (existingMetricsSpec == null || !Arrays.deepEquals(configuredMetricsSpec, existingMetricsSpec)) {
        return CompactionStatus.configChanged(
            "metricsSpec",
            configuredMetricsSpec,
            existingMetricsSpec,
            Arrays::toString
        );
      } else {
        return COMPLETE;
      }
    }

    private CompactionStatus transformSpecFilterIsUpToDate(CompactionState lastCompactionState)
    {
      if (compactionConfig.getTransformSpec() == null) {
        return COMPLETE;
      }

      CompactionTransformSpec existingTransformSpec = lastCompactionState.getTransformSpec();
      return CompactionStatus.completeIfNullOrEqual(
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
    private CompactionStatus evaluateForAllCompactionStates(
        Function<CompactionState, CompactionStatus> check
    )
    {
      CompactionStatus firstIncompleteStatus = null;
      for (CompactionState state : List.copyOf(unknownStateToSegments.keySet())) {
        final CompactionStatus status = check.apply(state);
        if (!status.isComplete()) {
          uncompactedSegments.addAll(unknownStateToSegments.remove(state));
          if (firstIncompleteStatus == null) {
            firstIncompleteStatus = status;
          }
        }
      }

      return firstIncompleteStatus == null ? COMPLETE : firstIncompleteStatus;
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
}
