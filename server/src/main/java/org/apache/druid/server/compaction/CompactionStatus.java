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
import org.apache.commons.lang3.ArrayUtils;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.client.indexing.ClientCompactionTaskTransformSpec;
import org.apache.druid.common.config.Configs;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents the status of compaction for a given {@link CompactionCandidate}.
 */
public class CompactionStatus
{
  private static final CompactionStatus COMPLETE = new CompactionStatus(State.COMPLETE, null);

  public enum State
  {
    COMPLETE, PENDING, RUNNING, SKIPPED
  }

  /**
   * List of checks performed to determine if compaction is already complete.
   * <p>
   * The order of the checks must be honored while evaluating them.
   */
  private static final List<Function<Evaluator, CompactionStatus>> CHECKS = Arrays.asList(
      Evaluator::segmentsHaveBeenCompactedAtLeastOnce,
      Evaluator::allCandidatesHaveSameCompactionState,
      Evaluator::partitionsSpecIsUpToDate,
      Evaluator::indexSpecIsUpToDate,
      Evaluator::segmentGranularityIsUpToDate,
      Evaluator::queryGranularityIsUpToDate,
      Evaluator::rollupIsUpToDate,
      Evaluator::dimensionsSpecIsUpToDate,
      Evaluator::metricsSpecIsUpToDate,
      Evaluator::transformSpecFilterIsUpToDate
  );

  private final State state;
  private final String reason;

  private CompactionStatus(State state, String reason)
  {
    this.state = state;
    this.reason = reason;
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

  @Override
  public String toString()
  {
    return "CompactionStatus{" +
           "state=" + state +
           ", reason=" + reason +
           '}';
  }

  private static CompactionStatus incomplete(String reasonFormat, Object... args)
  {
    return new CompactionStatus(State.PENDING, StringUtils.format(reasonFormat, args));
  }

  private static <T> CompactionStatus completeIfEqual(
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
    return CompactionStatus.incomplete(
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

  static CompactionStatus skipped(String reasonFormat, Object... args)
  {
    return new CompactionStatus(State.SKIPPED, StringUtils.format(reasonFormat, args));
  }

  static CompactionStatus running(String reasonForCompaction)
  {
    return new CompactionStatus(State.RUNNING, reasonForCompaction);
  }

  /**
   * Determines the CompactionStatus of the given candidate segments by evaluating
   * the {@link #CHECKS} one by one. If any check returns an incomplete status,
   * further checks are not performed and the incomplete status is returned.
   */
  static CompactionStatus compute(
      CompactionCandidate candidateSegments,
      DataSourceCompactionConfig config,
      ObjectMapper objectMapper
  )
  {
    final Evaluator evaluator = new Evaluator(candidateSegments, config, objectMapper);
    return CHECKS.stream().map(f -> f.apply(evaluator))
                 .filter(status -> !status.isComplete())
                 .findFirst().orElse(COMPLETE);
  }

  static PartitionsSpec findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig tuningConfig)
  {
    final PartitionsSpec partitionsSpecFromTuningConfig = tuningConfig.getPartitionsSpec();
    if (partitionsSpecFromTuningConfig == null) {
      final long maxTotalRows = Configs.valueOrDefault(tuningConfig.getMaxTotalRows(), Long.MAX_VALUE);
      return new DynamicPartitionsSpec(tuningConfig.getMaxRowsPerSegment(), maxTotalRows);
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

  private static List<DimensionSchema> getNonPartitioningDimensions(
      @Nullable final List<DimensionSchema> dimensionSchemas,
      @Nullable final PartitionsSpec partitionsSpec
  )
  {
    if (dimensionSchemas == null || !(partitionsSpec instanceof DimensionRangePartitionsSpec)) {
      return dimensionSchemas;
    }

    final List<String> partitionsDimensions = ((DimensionRangePartitionsSpec) partitionsSpec).getPartitionDimensions();
    return dimensionSchemas.stream()
                     .filter(dim -> !partitionsDimensions.contains(dim.getName()))
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
   * Evaluates {@link #CHECKS} to determine the compaction status.
   */
  private static class Evaluator
  {
    private final ObjectMapper objectMapper;
    private final DataSourceCompactionConfig compactionConfig;
    private final CompactionCandidate candidateSegments;
    private final CompactionState lastCompactionState;
    private final ClientCompactionTaskQueryTuningConfig tuningConfig;
    private final ClientCompactionTaskGranularitySpec existingGranularitySpec;
    private final UserCompactionTaskGranularityConfig configuredGranularitySpec;

    private Evaluator(
        CompactionCandidate candidateSegments,
        DataSourceCompactionConfig compactionConfig,
        ObjectMapper objectMapper
    )
    {
      this.candidateSegments = candidateSegments;
      this.objectMapper = objectMapper;
      this.lastCompactionState = candidateSegments.getSegments().get(0).getLastCompactionState();
      this.compactionConfig = compactionConfig;
      this.tuningConfig = ClientCompactionTaskQueryTuningConfig.from(compactionConfig);
      this.configuredGranularitySpec = compactionConfig.getGranularitySpec();
      if (lastCompactionState == null) {
        this.existingGranularitySpec = null;
      } else {
        this.existingGranularitySpec = convertIfNotNull(
            lastCompactionState.getGranularitySpec(),
            ClientCompactionTaskGranularitySpec.class
        );
      }
    }

    private CompactionStatus segmentsHaveBeenCompactedAtLeastOnce()
    {
      if (lastCompactionState == null) {
        return CompactionStatus.incomplete("not compacted yet");
      } else {
        return COMPLETE;
      }
    }

    private CompactionStatus allCandidatesHaveSameCompactionState()
    {
      final boolean allHaveSameCompactionState = candidateSegments.getSegments().stream().allMatch(
          segment -> lastCompactionState.equals(segment.getLastCompactionState())
      );
      if (allHaveSameCompactionState) {
        return COMPLETE;
      } else {
        return CompactionStatus.incomplete("segments have different last compaction states");
      }
    }

    private CompactionStatus partitionsSpecIsUpToDate()
    {
      PartitionsSpec existingPartionsSpec = lastCompactionState.getPartitionsSpec();
      if (existingPartionsSpec instanceof DimensionRangePartitionsSpec) {
        existingPartionsSpec = getEffectiveRangePartitionsSpec((DimensionRangePartitionsSpec) existingPartionsSpec);
      }
      return CompactionStatus.completeIfEqual(
          "partitionsSpec",
          findPartitionsSpecFromConfig(tuningConfig),
          existingPartionsSpec,
          CompactionStatus::asString
      );
    }

    private CompactionStatus indexSpecIsUpToDate()
    {
      return CompactionStatus.completeIfEqual(
          "indexSpec",
          Configs.valueOrDefault(tuningConfig.getIndexSpec(), IndexSpec.DEFAULT),
          objectMapper.convertValue(lastCompactionState.getIndexSpec(), IndexSpec.class),
          String::valueOf
      );
    }

    private CompactionStatus segmentGranularityIsUpToDate()
    {
      if (configuredGranularitySpec == null
          || configuredGranularitySpec.getSegmentGranularity() == null) {
        return COMPLETE;
      }

      final Granularity configuredSegmentGranularity = configuredGranularitySpec.getSegmentGranularity();
      final Granularity existingSegmentGranularity
          = existingGranularitySpec == null ? null : existingGranularitySpec.getSegmentGranularity();

      if (configuredSegmentGranularity.equals(existingSegmentGranularity)) {
        return COMPLETE;
      } else if (existingSegmentGranularity == null) {
        // Candidate segments were compacted without segment granularity specified
        // Check if the segments already have the desired segment granularity
        boolean needsCompaction = candidateSegments.getSegments().stream().anyMatch(
            segment -> !configuredSegmentGranularity.isAligned(segment.getInterval())
        );
        if (needsCompaction) {
          return CompactionStatus.incomplete(
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

    private CompactionStatus rollupIsUpToDate()
    {
      if (configuredGranularitySpec == null) {
        return COMPLETE;
      } else {
        return CompactionStatus.completeIfEqual(
            "rollup",
            configuredGranularitySpec.isRollup(),
            existingGranularitySpec == null ? null : existingGranularitySpec.isRollup(),
            String::valueOf
        );
      }
    }

    private CompactionStatus queryGranularityIsUpToDate()
    {
      if (configuredGranularitySpec == null) {
        return COMPLETE;
      } else {
        return CompactionStatus.completeIfEqual(
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
    private CompactionStatus dimensionsSpecIsUpToDate()
    {
      if (compactionConfig.getDimensionsSpec() == null) {
        return COMPLETE;
      } else {
        List<DimensionSchema> existingDimensions = getNonPartitioningDimensions(
            lastCompactionState.getDimensionsSpec() == null
            ? null
            : lastCompactionState.getDimensionsSpec().getDimensions(),
            lastCompactionState.getPartitionsSpec()
        );
        List<DimensionSchema> configuredDimensions = getNonPartitioningDimensions(
            compactionConfig.getDimensionsSpec().getDimensions(),
            compactionConfig.getTuningConfig() == null ? null : compactionConfig.getTuningConfig().getPartitionsSpec()
        );
        {
          return CompactionStatus.completeIfEqual(
              "dimensionsSpec",
              configuredDimensions,
              existingDimensions,
              String::valueOf
          );
        }
      }
    }

    private CompactionStatus metricsSpecIsUpToDate()
    {
      final AggregatorFactory[] configuredMetricsSpec = compactionConfig.getMetricsSpec();
      if (ArrayUtils.isEmpty(configuredMetricsSpec)) {
        return COMPLETE;
      }

      final List<Object> metricSpecList = lastCompactionState.getMetricsSpec();
      final AggregatorFactory[] existingMetricsSpec
          = CollectionUtils.isNullOrEmpty(metricSpecList)
            ? null : objectMapper.convertValue(metricSpecList, AggregatorFactory[].class);

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

    private CompactionStatus transformSpecFilterIsUpToDate()
    {
      if (compactionConfig.getTransformSpec() == null) {
        return COMPLETE;
      }

      ClientCompactionTaskTransformSpec existingTransformSpec = convertIfNotNull(
          lastCompactionState.getTransformSpec(),
          ClientCompactionTaskTransformSpec.class
      );
      return CompactionStatus.completeIfEqual(
          "transformSpec filter",
          compactionConfig.getTransformSpec().getFilter(),
          existingTransformSpec == null ? null : existingTransformSpec.getFilter(),
          String::valueOf
      );
    }

    @Nullable
    private <T> T convertIfNotNull(Object object, Class<T> clazz)
    {
      if (object == null) {
        return null;
      } else {
        return objectMapper.convertValue(object, clazz);
      }
    }
  }
}
