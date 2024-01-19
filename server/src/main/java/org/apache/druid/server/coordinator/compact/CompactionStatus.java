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

package org.apache.druid.server.coordinator.compact;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.ArrayUtils;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.client.indexing.ClientCompactionTaskTransformSpec;
import org.apache.druid.common.config.Configs;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
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

/**
 * Represents the status of compaction for a given list of candidate segments.
 */
public class CompactionStatus
{
  private static final CompactionStatus COMPLETE = new CompactionStatus(true, null);

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

  private final boolean complete;
  private final String reasonToCompact;

  private CompactionStatus(boolean complete, String reason)
  {
    this.complete = complete;
    this.reasonToCompact = reason;
  }

  public boolean isComplete()
  {
    return complete;
  }

  public String getReasonToCompact()
  {
    return reasonToCompact;
  }

  private static CompactionStatus incomplete(String reasonFormat, Object... args)
  {
    return new CompactionStatus(false, StringUtils.format(reasonFormat, args));
  }

  private static CompactionStatus completeIfEqual(String field, Object configured, Object current)
  {
    if (configured == null || configured.equals(current)) {
      return COMPLETE;
    } else {
      return configChanged(field, configured, current);
    }
  }

  private static CompactionStatus configChanged(String field, Object configured, Object current)
  {
    return CompactionStatus.incomplete(
        "Configured %s[%s] is different from current %s[%s]",
        field, configured, field, current
    );
  }

  /**
   * Determines the CompactionStatus of the given candidate segments by evaluating
   * the {@link #CHECKS} one by one. If any check returns an incomplete status,
   * further checks are not performed and the incomplete status is returned.
   */
  static CompactionStatus of(
      SegmentsToCompact candidateSegments,
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
    } else {
      return partitionsSpecFromTuningConfig;
    }
  }

  /**
   * Evaluates {@link #CHECKS} to determine the compaction status.
   */
  private static class Evaluator
  {
    private final ObjectMapper objectMapper;
    private final DataSourceCompactionConfig compactionConfig;
    private final SegmentsToCompact candidateSegments;
    private final CompactionState lastCompactionState;
    private final ClientCompactionTaskQueryTuningConfig tuningConfig;
    private final ClientCompactionTaskGranularitySpec existingGranularitySpec;
    private final UserCompactionTaskGranularityConfig configuredGranularitySpec;

    private Evaluator(
        SegmentsToCompact candidateSegments,
        DataSourceCompactionConfig compactionConfig,
        ObjectMapper objectMapper
    )
    {
      Preconditions.checkArgument(!candidateSegments.isEmpty(), "Empty candidates");

      this.candidateSegments = candidateSegments;
      this.objectMapper = objectMapper;
      this.lastCompactionState = candidateSegments.getFirst().getLastCompactionState();
      this.compactionConfig = compactionConfig;
      this.tuningConfig = ClientCompactionTaskQueryTuningConfig.from(
          compactionConfig.getTuningConfig(),
          compactionConfig.getMaxRowsPerSegment(),
          null
      );

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
        return CompactionStatus.incomplete("Not compacted yet");
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
        return CompactionStatus.incomplete("Candidate segments have different last compaction states.");
      }
    }

    private CompactionStatus partitionsSpecIsUpToDate()
    {
      return CompactionStatus.completeIfEqual(
          "partitionsSpec",
          findPartitionsSpecFromConfig(tuningConfig),
          lastCompactionState.getPartitionsSpec()
      );
    }

    private CompactionStatus indexSpecIsUpToDate()
    {
      return CompactionStatus.completeIfEqual(
          "indexSpec",
          Configs.valueOrDefault(tuningConfig.getIndexSpec(), IndexSpec.DEFAULT),
          objectMapper.convertValue(lastCompactionState.getIndexSpec(), IndexSpec.class)
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
              "Configured segmentGranularity[%s] does not align with segment intervals.",
              configuredSegmentGranularity
          );
        }
      } else {
        return CompactionStatus.configChanged(
            "segmentGranularity",
            configuredSegmentGranularity,
            existingSegmentGranularity
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
            existingGranularitySpec == null ? null : existingGranularitySpec.isRollup()
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
            existingGranularitySpec == null ? null : existingGranularitySpec.getQueryGranularity()
        );
      }
    }

    private CompactionStatus dimensionsSpecIsUpToDate()
    {
      if (compactionConfig.getDimensionsSpec() == null) {
        return COMPLETE;
      } else {
        final DimensionsSpec existingDimensionsSpec = lastCompactionState.getDimensionsSpec();
        return CompactionStatus.completeIfEqual(
            "dimensionsSpec",
            compactionConfig.getDimensionsSpec().getDimensions(),
            existingDimensionsSpec == null ? null : existingDimensionsSpec.getDimensions()
        );
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
            Arrays.toString(configuredMetricsSpec),
            Arrays.toString(existingMetricsSpec)
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
          existingTransformSpec == null ? null : existingTransformSpec.getFilter()
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
