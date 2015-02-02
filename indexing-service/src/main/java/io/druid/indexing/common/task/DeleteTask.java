/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import io.druid.granularity.QueryGranularity;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexableAdapter;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexAdapter;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;

public class DeleteTask extends AbstractFixedIntervalTask
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
    final TaskLock myLock = Iterables.getOnlyElement(getTaskLocks(toolbox));
    final IncrementalIndex empty = new IncrementalIndex(0, QueryGranularity.NONE, new AggregatorFactory[0]);
    final IndexableAdapter emptyAdapter = new IncrementalIndexAdapter(getInterval(), empty);

    // Create DataSegment
    final DataSegment segment =
        DataSegment.builder()
                   .dataSource(this.getDataSource())
                   .interval(getInterval())
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

    toolbox.pushSegments(ImmutableList.of(uploadedSegment));

    return TaskStatus.success(getId());
  }
}
