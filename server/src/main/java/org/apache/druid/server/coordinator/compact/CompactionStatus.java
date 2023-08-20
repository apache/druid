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
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Represents the status of compaction for a given list of candidate segments.
 * Compaction may be considered incomplete if any of the following is true:
 * <ul>
 * <li>Segments have never been compacted</li>
 * <li>Segments have different last compaction states</li>
 * <li>
 *   Configured value of any of the following parameters is different from its
 *   current value (as determined from the segments):
 *   <ul>
 *     <li>Partitions spec</li>
 *     <li>Index spec</li>
 *     <li>Segment granularity</li>
 *     <li>Query granularity</li>
 *     <li>Rollup</li>
 *     <li>Dimensions spec</li>
 *     <li>Metrics spec</li>
 *     <li>Filters in transform spec</li>
 *   </ul>
 * </li>
 * </ul>
 */
public class CompactionStatus
{
  private static final CompactionStatus COMPLETE = new CompactionStatus(true, null);

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

  private static CompactionStatus compareConfigs(String field, Object configured, Object current)
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
   * Determines the CompactionStatus of the given candidate segments by comparing
   * them with the compaction config.
   *
   * @see CompactionStatus
   */
  static CompactionStatus of(
      SegmentsToCompact candidateSegments,
      DataSourceCompactionConfig config,
      ObjectMapper objectMapper
  )
  {
    return new Evaluator(candidateSegments, config, objectMapper).evaluate();
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

    /**
     * Compares all configured fields with the current compaction state of the
     * segments to determine the CompactionStatus.
     */
    private CompactionStatus evaluate()
    {
      if (lastCompactionState == null) {
        return CompactionStatus.incomplete("Not compacted yet");
      }

      final boolean allHaveSameCompactionState = candidateSegments.getSegments().stream().allMatch(
          segment -> lastCompactionState.equals(segment.getLastCompactionState())
      );
      if (!allHaveSameCompactionState) {
        return CompactionStatus.incomplete("Candidate segments have different last compaction states.");
      }

      // Compare all fields to determine if compaction is required
      final Stream<Supplier<CompactionStatus>> checks = Stream.of(
          this::comparePartitionsSpec,
          this::compareIndexSpec,
          this::compareSegmentGranularity,
          this::compareQueryGranularity,
          this::compareRollup,
          this::compareDimensionsSpec,
          this::compareMetricsSpec,
          this::compareTransformsSpecFilters
      );
      return checks.map(Supplier::get)
                   .filter(status -> !status.isComplete())
                   .findFirst().orElse(COMPLETE);
    }

    private CompactionStatus comparePartitionsSpec()
    {
      return CompactionStatus.compareConfigs(
          "partitionsSpec",
          findPartitionsSpecFromConfig(tuningConfig),
          lastCompactionState.getPartitionsSpec()
      );
    }

    private CompactionStatus compareIndexSpec()
    {
      return CompactionStatus.compareConfigs(
          "indexSpec",
          Configs.valueOrDefault(tuningConfig.getIndexSpec(), IndexSpec.DEFAULT),
          objectMapper.convertValue(lastCompactionState.getIndexSpec(), IndexSpec.class)
      );
    }

    private CompactionStatus compareSegmentGranularity()
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

    private CompactionStatus compareRollup()
    {
      if (configuredGranularitySpec == null) {
        return COMPLETE;
      } else {
        return CompactionStatus.compareConfigs(
            "rollup",
            configuredGranularitySpec.isRollup(),
            existingGranularitySpec == null ? null : existingGranularitySpec.isRollup()
        );
      }
    }

    private CompactionStatus compareQueryGranularity()
    {
      if (configuredGranularitySpec == null) {
        return COMPLETE;
      } else {
        return CompactionStatus.compareConfigs(
            "queryGranularity",
            configuredGranularitySpec.getQueryGranularity(),
            existingGranularitySpec == null ? null : existingGranularitySpec.getQueryGranularity()
        );
      }
    }

    private CompactionStatus compareDimensionsSpec()
    {
      if (compactionConfig.getDimensionsSpec() == null) {
        return COMPLETE;
      } else {
        final DimensionsSpec existingDimensionsSpec = lastCompactionState.getDimensionsSpec();
        return CompactionStatus.compareConfigs(
            "dimensionsSpec",
            compactionConfig.getDimensionsSpec().getDimensions(),
            existingDimensionsSpec == null ? null : existingDimensionsSpec.getDimensions()
        );
      }
    }

    private CompactionStatus compareMetricsSpec()
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

    private CompactionStatus compareTransformsSpecFilters()
    {
      if (compactionConfig.getTransformSpec() == null) {
        return COMPLETE;
      }

      ClientCompactionTaskTransformSpec existingTransformSpec = convertIfNotNull(
          lastCompactionState.getTransformSpec(),
          ClientCompactionTaskTransformSpec.class
      );
      return CompactionStatus.compareConfigs(
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
