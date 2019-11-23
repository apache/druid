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

package org.apache.druid.indexing.common.task.batch.parallel;

import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PerfectRollupWorkerTaskTest
{
  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void requiresForceGuaranteedRollup()
  {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("forceGuaranteedRollup must be set");

    new PerfectRollupWorkerTaskBuilder()
        .forceGuaranteedRollup(false)
        .build();
  }

  @Test
  public void failsWithInvalidPartitionsSpec()
  {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("forceGuaranteedRollup is incompatible with partitionsSpec");

    new PerfectRollupWorkerTaskBuilder()
        .partitionsSpec(HashedPartitionsSpec.defaultSpec())
        .build();
  }

  @Test
  public void requiresGranularitySpecInputIntervals()
  {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Missing intervals in granularitySpec");

    new PerfectRollupWorkerTaskBuilder()
        .granularitySpecInputIntervals(Collections.emptyList())
        .build();
  }

  @Test
  public void succeedsWithValidPartitionsSpec()
  {
    new PerfectRollupWorkerTaskBuilder().build();
  }

  @SuppressWarnings("SameParameterValue")
  private static class PerfectRollupWorkerTaskBuilder
  {
    private static final PartitionsSpec PARTITIONS_SPEC = new HashedPartitionsSpec(
        null,
        1,
        null,
        null,
        null
    );

    private List<Interval> granularitySpecInputIntervals = Collections.singletonList(Intervals.ETERNITY);
    private boolean forceGuaranteedRollup = true;
    private PartitionsSpec partitionsSpec = PARTITIONS_SPEC;

    PerfectRollupWorkerTaskBuilder granularitySpecInputIntervals(List<Interval> granularitySpecInputIntervals)
    {
      this.granularitySpecInputIntervals = granularitySpecInputIntervals;
      return this;
    }

    PerfectRollupWorkerTaskBuilder forceGuaranteedRollup(boolean forceGuaranteedRollup)
    {
      this.forceGuaranteedRollup = forceGuaranteedRollup;
      return this;
    }

    PerfectRollupWorkerTaskBuilder partitionsSpec(PartitionsSpec partitionsSpec)
    {
      this.partitionsSpec = partitionsSpec;
      return this;
    }

    PerfectRollupWorkerTask build()
    {
      return new TestPerfectRollupWorkerTask(
          "id",
          "group-id",
          null,
          createDataSchema(granularitySpecInputIntervals),
          createTuningConfig(forceGuaranteedRollup, partitionsSpec),
          null
      );
    }

    private static DataSchema createDataSchema(List<Interval> granularitySpecInputIntervals)
    {
      GranularitySpec granularitySpec = EasyMock.mock(GranularitySpec.class);
      EasyMock.expect(granularitySpec.inputIntervals()).andStubReturn(granularitySpecInputIntervals);
      EasyMock.replay(granularitySpec);

      DataSchema dataSchema = EasyMock.mock(DataSchema.class);
      EasyMock.expect(dataSchema.getDataSource()).andStubReturn("datasource");
      EasyMock.expect(dataSchema.getGranularitySpec()).andStubReturn(granularitySpec);
      EasyMock.replay(dataSchema);
      return dataSchema;
    }

    private static ParallelIndexTuningConfig createTuningConfig(
        boolean forceGuaranteedRollup,
        PartitionsSpec partitionsSpec
    )
    {
      ParallelIndexTuningConfig tuningConfig = EasyMock.mock(ParallelIndexTuningConfig.class);
      EasyMock.expect(tuningConfig.isForceGuaranteedRollup()).andStubReturn(forceGuaranteedRollup);
      EasyMock.expect(tuningConfig.getGivenOrDefaultPartitionsSpec()).andStubReturn(partitionsSpec);
      EasyMock.replay(tuningConfig);
      return tuningConfig;
    }
  }

  private static class TestPerfectRollupWorkerTask extends PerfectRollupWorkerTask
  {
    TestPerfectRollupWorkerTask(
        String id,
        @Nullable String groupId,
        @Nullable TaskResource taskResource,
        DataSchema dataSchema,
        ParallelIndexTuningConfig tuningConfig,
        @Nullable Map<String, Object> context
    )
    {
      super(id, groupId, taskResource, dataSchema, tuningConfig, context);
    }

    @Override
    public TaskStatus runTask(TaskToolbox toolbox)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getType()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient)
    {
      throw new UnsupportedOperationException();
    }
  }
}
