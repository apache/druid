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

import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.output.OutputDestination;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.input.DruidDatasourceDestination;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.server.compaction.CompactionSlotManager;
import org.apache.druid.server.compaction.DataSourceCompactibleSegmentIterator;
import org.apache.druid.server.compaction.NewestSegmentFirstPolicy;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This template never needs to be deserialized as a {@code BatchIndexingJobTemplate}.
 * It is just a delegating template that uses a {@link DataSourceCompactionConfig}
 * to create compaction jobs.
 */
public class CompactionConfigBasedJobTemplate extends CompactionJobTemplate
{
  private final DataSourceCompactionConfig config;

  public CompactionConfigBasedJobTemplate(DataSourceCompactionConfig config)
  {
    this.config = config;
  }

  @Override
  public List<CompactionJob> createCompactionJobs(
      InputSource source,
      OutputDestination destination,
      CompactionJobParams params
  )
  {
    validateInput(source);
    validateOutput(destination);

    final Interval searchInterval = Objects.requireNonNull(ensureDruidInputSource(source).getInterval());

    final SegmentTimeline timeline = params.getTimeline(config.getDataSource());
    final DataSourceCompactibleSegmentIterator segmentIterator = new DataSourceCompactibleSegmentIterator(
        config,
        timeline,
        Intervals.complementOf(searchInterval),
        new NewestSegmentFirstPolicy(null)
    );

    final List<CompactionJob> jobs = new ArrayList<>();

    // Create a job for each CompactionCandidate
    while (segmentIterator.hasNext()) {
      final CompactionCandidate candidate = segmentIterator.next();

      ClientCompactionTaskQuery taskPayload
          = CompactSegments.createCompactionTask(candidate, config, params.getClusterCompactionConfig().getEngine());
      final Interval compactionInterval = taskPayload.getIoConfig().getInputSpec().getInterval();
      jobs.add(
          new CompactionJob(
              taskPayload,
              candidate,
              compactionInterval,
              CompactionSlotManager.getMaxTaskSlotsForNativeCompactionTask(taskPayload.getTuningConfig())
          )
      );
    }

    return jobs;
  }

  @Override
  public String getType()
  {
    throw new UnsupportedOperationException("This template type cannot be serialized");
  }

  private void validateInput(InputSource source)
  {
    final DruidInputSource druidInputSource = ensureDruidInputSource(source);
    if (!druidInputSource.getDataSource().equals(config.getDataSource())) {
      throw InvalidInput.exception(
          "Datasource[%s] in compaction config does not match datasource[%s] in input source",
          config.getDataSource(), druidInputSource.getDataSource()
      );
    }
  }

  private void validateOutput(OutputDestination destination)
  {
    final DruidDatasourceDestination druidDestination = ensureDruidDataSourceDestination(destination);
    if (!druidDestination.getDataSource().equals(config.getDataSource())) {
      throw InvalidInput.exception(
          "Datasource[%s] in compaction config does not match datasource[%s] in output destination",
          config.getDataSource(), druidDestination.getDataSource()
      );
    }
  }
}
