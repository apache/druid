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

package com.metamx.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.index.v1.SpatialDimensionSchema;
import com.metamx.druid.indexer.granularity.GranularitySpec;
import com.metamx.druid.indexing.common.TaskStatus;
import com.metamx.druid.indexing.common.TaskToolbox;
import com.metamx.druid.indexing.common.actions.SpawnTasksAction;
import com.metamx.druid.indexing.common.actions.TaskActionClient;
import com.metamx.druid.realtime.firehose.FirehoseFactory;
import com.metamx.druid.realtime.Schema;
import com.metamx.druid.realtime.firehose.FirehoseFactory;
import com.metamx.druid.shard.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;

public class IndexTask extends AbstractTask
{
  @JsonIgnore
  private final GranularitySpec granularitySpec;

  @JsonProperty
  private final List<SpatialDimensionSchema> spatialDimensions;

  @JsonIgnore
  private final AggregatorFactory[] aggregators;

  @JsonIgnore
  private final QueryGranularity indexGranularity;

  @JsonIgnore
  private final long targetPartitionSize;

  @JsonIgnore
  private final FirehoseFactory firehoseFactory;

  @JsonIgnore
  private final int rowFlushBoundary;

  private static final Logger log = new Logger(IndexTask.class);

  @JsonCreator
  public IndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("granularitySpec") GranularitySpec granularitySpec,
      @JsonProperty("spatialDimensions") List<SpatialDimensionSchema> spatialDimensions,
      @JsonProperty("aggregators") AggregatorFactory[] aggregators,
      @JsonProperty("indexGranularity") QueryGranularity indexGranularity,
      @JsonProperty("targetPartitionSize") long targetPartitionSize,
      @JsonProperty("firehose") FirehoseFactory firehoseFactory,
      @JsonProperty("rowFlushBoundary") int rowFlushBoundary
  )
  {
    super(
        // _not_ the version, just something uniqueish
        id != null ? id : String.format("index_%s_%s", dataSource, new DateTime().toString()),
        dataSource,
        new Interval(
            granularitySpec.bucketIntervals().first().getStart(),
            granularitySpec.bucketIntervals().last().getEnd()
        )
    );

    this.granularitySpec = Preconditions.checkNotNull(granularitySpec, "granularitySpec");
    this.spatialDimensions = (spatialDimensions == null)
                             ? Lists.<SpatialDimensionSchema>newArrayList()
                             : spatialDimensions;
    this.aggregators = aggregators;
    this.indexGranularity = indexGranularity;
    this.targetPartitionSize = targetPartitionSize;
    this.firehoseFactory = firehoseFactory;
    this.rowFlushBoundary = rowFlushBoundary;
  }

  public List<Task> toSubtasks()
  {
    final List<Task> retVal = Lists.newArrayList();

    for (final Interval interval : granularitySpec.bucketIntervals()) {
      if (targetPartitionSize > 0) {
        // Need to do one pass over the data before indexing in order to determine good partitions
        retVal.add(
            new IndexDeterminePartitionsTask(
                null,
                getGroupId(),
                interval,
                firehoseFactory,
                new Schema(
                    getDataSource(),
                    spatialDimensions,
                    aggregators,
                    indexGranularity,
                    new NoneShardSpec()
                ),
                targetPartitionSize,
                rowFlushBoundary
            )
        );
      } else {
        // Jump straight into indexing
        retVal.add(
            new IndexGeneratorTask(
                null,
                getGroupId(),
                interval,
                firehoseFactory,
                new Schema(
                    getDataSource(),
                    spatialDimensions,
                    aggregators,
                    indexGranularity,
                    new NoneShardSpec()
                ),
                rowFlushBoundary
            )
        );
      }
    }

    return retVal;
  }

  @Override
  public String getType()
  {
    return "index";
  }

  @Override
  public TaskStatus preflight(TaskActionClient taskActionClient) throws Exception
  {
    taskActionClient.submit(new SpawnTasksAction(toSubtasks()));
    return TaskStatus.success(getId());
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    throw new IllegalStateException("IndexTasks should not be run!");
  }

  @JsonProperty
  public GranularitySpec getGranularitySpec()
  {
    return granularitySpec;
  }

  @JsonProperty
  public AggregatorFactory[] getAggregators()
  {
    return aggregators;
  }

  @JsonProperty
  public QueryGranularity getIndexGranularity()
  {
    return indexGranularity;
  }

  @JsonProperty
  public long getTargetPartitionSize()
  {
    return targetPartitionSize;
  }

  @JsonProperty
  public FirehoseFactory getFirehoseFactory()
  {
    return firehoseFactory;
  }

  @JsonProperty
  public int getRowFlushBoundary()
  {
    return rowFlushBoundary;
  }

}
