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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.v1.IncrementalIndex;
import com.metamx.druid.index.v1.IncrementalIndexAdapter;
import com.metamx.druid.index.v1.IndexMerger;
import com.metamx.druid.index.v1.IndexableAdapter;
import com.metamx.druid.indexing.common.TaskLock;
import com.metamx.druid.indexing.common.TaskStatus;
import com.metamx.druid.indexing.common.TaskToolbox;
import com.metamx.druid.indexing.common.actions.LockListAction;
import com.metamx.druid.indexing.common.actions.SegmentInsertAction;
import com.metamx.druid.shard.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;

public class DeleteTask extends AbstractTask
{
  private static final Logger log = new Logger(DeleteTask.class);

  @JsonCreator
  public DeleteTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval
  )
  {
    super(
        id != null ? id : String.format(
            "delete_%s_%s_%s_%s",
            dataSource,
            interval.getStart(),
            interval.getEnd(),
            new DateTime().toString()
        ),
        dataSource,
        Preconditions.checkNotNull(interval, "interval")
    );
  }

  @Override
  public String getType()
  {
    return "delete";
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    // Strategy: Create an empty segment covering the interval to be deleted
    final TaskLock myLock = Iterables.getOnlyElement(toolbox.getTaskActionClient().submit(new LockListAction()));
    final Interval interval = this.getImplicitLockInterval().get();
    final IncrementalIndex empty = new IncrementalIndex(0, QueryGranularity.NONE, new AggregatorFactory[0]);
    final IndexableAdapter emptyAdapter = new IncrementalIndexAdapter(interval, empty);

    // Create DataSegment
    final DataSegment segment =
        DataSegment.builder()
                   .dataSource(this.getDataSource())
                   .interval(interval)
                   .version(myLock.getVersion())
                   .shardSpec(new NoneShardSpec())
                   .build();

    final File outDir = new File(toolbox.getTaskWorkDir(), segment.getIdentifier());
    final File fileToUpload = IndexMerger.merge(Lists.newArrayList(emptyAdapter), new AggregatorFactory[0], outDir);

    // Upload the segment
    final DataSegment uploadedSegment = toolbox.getSegmentPusher().push(fileToUpload, segment);

    log.info(
        "Uploaded tombstone segment for[%s] interval[%s] with version[%s]",
        segment.getDataSource(),
        segment.getInterval(),
        segment.getVersion()
    );

    toolbox.getTaskActionClient().submit(new SegmentInsertAction(ImmutableSet.of(uploadedSegment)));

    return TaskStatus.success(getId());
  }
}
