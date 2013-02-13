/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.merger.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.indexer.granularity.GranularitySpec;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.coordinator.TaskContext;
import com.metamx.druid.realtime.FirehoseFactory;
import com.metamx.druid.realtime.Schema;
import com.metamx.druid.shard.NoneShardSpec;


import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;

public class IndexTask extends AbstractTask
{
  @JsonProperty private final GranularitySpec granularitySpec;
  @JsonProperty private final AggregatorFactory[] aggregators;
  @JsonProperty private final QueryGranularity indexGranularity;
  @JsonProperty private final long targetPartitionSize;
  @JsonProperty private final FirehoseFactory firehoseFactory;

  private static final Logger log = new Logger(IndexTask.class);

  @JsonCreator
  public IndexTask(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("granularitySpec") GranularitySpec granularitySpec,
      @JsonProperty("aggregators") AggregatorFactory[] aggregators,
      @JsonProperty("indexGranularity") QueryGranularity indexGranularity,
      @JsonProperty("targetPartitionSize") long targetPartitionSize,
      @JsonProperty("firehose") FirehoseFactory firehoseFactory
  )
  {
    super(
        // _not_ the version, just something uniqueish
        String.format("index_%s_%s", dataSource, new DateTime().toString()),
        dataSource,
        new Interval(
            granularitySpec.bucketIntervals().first().getStart(),
            granularitySpec.bucketIntervals().last().getEnd()
        )
    );

    this.granularitySpec = Preconditions.checkNotNull(granularitySpec, "granularitySpec");
    this.aggregators = aggregators;
    this.indexGranularity = indexGranularity;
    this.targetPartitionSize = targetPartitionSize;
    this.firehoseFactory = firehoseFactory;
  }

  public List<Task> toSubtasks()
  {
    final List<Task> retVal = Lists.newArrayList();

    for (final Interval interval : granularitySpec.bucketIntervals()) {
      if (targetPartitionSize > 0) {
        // Need to do one pass over the data before indexing in order to determine good partitions
        retVal.add(
            new IndexDeterminePartitionsTask(
                getGroupId(),
                interval,
                firehoseFactory,
                new Schema(
                    getDataSource(),
                    aggregators,
                    indexGranularity,
                    new NoneShardSpec()
                ),
                targetPartitionSize
            )
        );
      } else {
        // Jump straight into indexing
        retVal.add(
            new IndexGeneratorTask(
                getGroupId(),
                interval,
                firehoseFactory,
                new Schema(
                    getDataSource(),
                    aggregators,
                    indexGranularity,
                    new NoneShardSpec()
                )
            )
        );
      }
    }

    return retVal;
  }

  @Override
  public Type getType()
  {
    return Type.INDEX;
  }

  @Override
  public TaskStatus preflight(TaskContext context) throws Exception
  {
    return TaskStatus.continued(getId(), toSubtasks());
  }

  @Override
  public TaskStatus run(TaskContext context, TaskToolbox toolbox) throws Exception
  {
    throw new IllegalStateException("IndexTasks should not be run!");
  }
}
