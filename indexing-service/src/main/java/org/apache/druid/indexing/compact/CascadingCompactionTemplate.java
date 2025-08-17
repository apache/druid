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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskIOConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Template to perform period-based cascading compaction. Contains a list of
 * {@link CompactionRule} which divide the segment timeline into compactible
 * intervals. Each rule specifies a period relative to the current time which is
 * used to determine its applicable interval:
 * <ul>
 * <li>Rule 1: range = [now - p1, +inf)</li>
 * <li>Rule 2: range = [now - p2, now - p1)</li>
 * <li>...</li>
 * <li>Rule n: range = (-inf, now - p(n - 1))</li>
 * </ul>
 *
 * If two adjacent rules explicitly specify a segment granularity, the boundary
 * between them may be {@linkplain CompactionRule#computeStartTime adjusted}
 * to ensure that there are no uncompacted gaps in the timeline.
 * <p>
 * This template never needs to be deserialized as a {@code BatchIndexingJobTemplate},
 * only as a {@link DataSourceCompactionConfig} in {@link CompactionSupervisorSpec}.
 */
public class CascadingCompactionTemplate implements CompactionJobTemplate, DataSourceCompactionConfig
{
  public static final String TYPE = "compactCascade";

  private final String dataSource;
  private final List<CompactionRule> rules;

  @JsonCreator
  public CascadingCompactionTemplate(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("rules") List<CompactionRule> rules
  )
  {
    this.rules = rules;
    this.dataSource = Objects.requireNonNull(dataSource, "'dataSource' cannot be null");

    InvalidInput.conditionalException(rules != null && !rules.isEmpty(), "'rules' cannot be empty");
  }

  @Override
  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public List<CompactionRule> getRules()
  {
    return rules;
  }

  @Override
  public List<CompactionJob> createCompactionJobs(
      DruidInputSource source,
      CompactionJobParams jobParams
  )
  {
    final List<CompactionJob> allJobs = new ArrayList<>();

    // Include future dates in the first rule
    final DateTime currentTime = jobParams.getScheduleStartTime();
    DateTime previousRuleStartTime = DateTimes.MAX;
    for (int i = 0; i < rules.size() - 1; ++i) {
      final CompactionRule rule = rules.get(i);
      final DateTime ruleStartTime = rule.computeStartTime(currentTime, rules.get(i + 1));
      final Interval ruleInterval = new Interval(ruleStartTime, previousRuleStartTime);

      allJobs.addAll(
          createJobsForSearchInterval(rule.getTemplate(), ruleInterval, source, jobParams)
      );

      previousRuleStartTime = ruleStartTime;
    }

    // Include past dates in the last rule
    final CompactionRule lastRule = rules.get(rules.size() - 1);
    final Interval lastRuleInterval = new Interval(DateTimes.MIN, previousRuleStartTime);
    allJobs.addAll(
        createJobsForSearchInterval(lastRule.getTemplate(), lastRuleInterval, source, jobParams)
    );

    return allJobs;
  }

  private List<CompactionJob> createJobsForSearchInterval(
      CompactionJobTemplate template,
      Interval searchInterval,
      DruidInputSource inputSource,
      CompactionJobParams jobParams
  )
  {
    final List<CompactionJob> allJobs = template.createCompactionJobs(
        inputSource.withInterval(searchInterval),
        jobParams
    );

    // Filter out jobs if they are outside the search interval
    final List<CompactionJob> validJobs = new ArrayList<>();
    for (CompactionJob job : allJobs) {
      final Interval compactionInterval = job.getCandidate().getCompactionInterval();
      if (searchInterval.contains(compactionInterval)) {
        validJobs.add(job);
      }
    }

    return validJobs;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  // Legacy fields from DataSourceCompactionConfig that are not used by this template

  @Nullable
  @Override
  public CompactionEngine getEngine()
  {
    return null;
  }

  @Override
  public int getTaskPriority()
  {
    return 0;
  }

  @Override
  public long getInputSegmentSizeBytes()
  {
    return 0;
  }

  @Nullable
  @Override
  public Integer getMaxRowsPerSegment()
  {
    return 0;
  }

  @Override
  public Period getSkipOffsetFromLatest()
  {
    return null;
  }

  @Nullable
  @Override
  public UserCompactionTaskQueryTuningConfig getTuningConfig()
  {
    return null;
  }

  @Nullable
  @Override
  public UserCompactionTaskIOConfig getIoConfig()
  {
    return null;
  }

  @Nullable
  @Override
  public Map<String, Object> getTaskContext()
  {
    return Map.of();
  }

  @Nullable
  @Override
  public Granularity getSegmentGranularity()
  {
    return null;
  }

  @Nullable
  @Override
  public UserCompactionTaskGranularityConfig getGranularitySpec()
  {
    return null;
  }

  @Nullable
  @Override
  public List<AggregateProjectionSpec> getProjections()
  {
    return List.of();
  }

  @Nullable
  @Override
  public CompactionTransformSpec getTransformSpec()
  {
    return null;
  }

  @Nullable
  @Override
  public UserCompactionTaskDimensionsConfig getDimensionsSpec()
  {
    return null;
  }

  @Nullable
  @Override
  public AggregatorFactory[] getMetricsSpec()
  {
    return new AggregatorFactory[0];
  }
}
