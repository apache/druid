/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.LockListAction;
import io.druid.indexing.common.actions.SegmentInsertAction;
import io.druid.indexing.common.index.YeOldePlumberSchool;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.Schema;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.segment.realtime.plumber.Sink;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class IndexGeneratorTask extends AbstractTask
{
  @JsonIgnore
  private final FirehoseFactory firehoseFactory;

  @JsonIgnore
  private final Schema schema;

  @JsonIgnore
  private final int rowFlushBoundary;

  private static final Logger log = new Logger(IndexTask.class);

  @JsonCreator
  public IndexGeneratorTask(
      @JsonProperty("id") String id,
      @JsonProperty("groupId") String groupId,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("firehose") FirehoseFactory firehoseFactory,
      @JsonProperty("schema") Schema schema,
      @JsonProperty("rowFlushBoundary") int rowFlushBoundary
  )
  {
    super(
        id != null
        ? id
        : String.format(
            "%s_generator_%s_%s_%s",
            groupId,
            interval.getStart(),
            interval.getEnd(),
            schema.getShardSpec().getPartitionNum()
        ),
        groupId,
        schema.getDataSource(),
        Preconditions.checkNotNull(interval, "interval")
    );

    this.firehoseFactory = firehoseFactory;
    this.schema = schema;
    this.rowFlushBoundary = rowFlushBoundary;
  }

  @Override
  public String getType()
  {
    return "index_generator";
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    // We should have a lock from before we started running
    final TaskLock myLock = Iterables.getOnlyElement(getTaskLocks(toolbox));

    // We know this exists
    final Interval interval = getImplicitLockInterval().get();

    // Set up temporary directory for indexing
    final File tmpDir = new File(
        toolbox.getTaskWorkDir(),
        String.format(
            "%s_%s_%s_%s_%s",
            this.getDataSource(),
            interval.getStart(),
            interval.getEnd(),
            myLock.getVersion(),
            schema.getShardSpec().getPartitionNum()
        )
    );

    // We need to track published segments.
    final List<DataSegment> pushedSegments = new CopyOnWriteArrayList<DataSegment>();
    final DataSegmentPusher wrappedDataSegmentPusher = new DataSegmentPusher()
    {
      @Override
      public DataSegment push(File file, DataSegment segment) throws IOException
      {
        final DataSegment pushedSegment = toolbox.getSegmentPusher().push(file, segment);
        pushedSegments.add(pushedSegment);
        return pushedSegment;
      }
    };

    // Create firehose + plumber
    final FireDepartmentMetrics metrics = new FireDepartmentMetrics();
    final Firehose firehose = firehoseFactory.connect();
    final Plumber plumber = new YeOldePlumberSchool(
        interval,
        myLock.getVersion(),
        wrappedDataSegmentPusher,
        tmpDir
    ).findPlumber(schema, metrics);

    // rowFlushBoundary for this job
    final int myRowFlushBoundary = this.rowFlushBoundary > 0
                                   ? rowFlushBoundary
                                   : toolbox.getConfig().getDefaultRowFlushBoundary();

    try {
      while (firehose.hasMore()) {
        final InputRow inputRow = firehose.nextRow();

        if (shouldIndex(inputRow)) {
          final Sink sink = plumber.getSink(inputRow.getTimestampFromEpoch());
          if (sink == null) {
            throw new NullPointerException(
                String.format(
                    "Was expecting non-null sink for timestamp[%s]",
                    new DateTime(inputRow.getTimestampFromEpoch())
                )
            );
          }

          int numRows = sink.add(inputRow);
          metrics.incrementProcessed();

          if (numRows >= myRowFlushBoundary) {
            plumber.persist(firehose.commit());
          }
        } else {
          metrics.incrementThrownAway();
        }
      }
    }
    finally {
      firehose.close();
    }

    plumber.persist(firehose.commit());
    plumber.finishJob();

    // Output metrics
    log.info(
        "Task[%s] took in %,d rows (%,d processed, %,d unparseable, %,d thrown away) and output %,d rows",
        getId(),
        metrics.processed() + metrics.unparseable() + metrics.thrownAway(),
        metrics.processed(),
        metrics.unparseable(),
        metrics.thrownAway(),
        metrics.rowOutput()
    );

    // Request segment pushes
    toolbox.getTaskActionClient().submit(new SegmentInsertAction(ImmutableSet.copyOf(pushedSegments)));

    // Done
    return TaskStatus.success(getId());
  }

  /**
   * Should we index this inputRow? Decision is based on our interval and shardSpec.
   *
   * @param inputRow the row to check
   *
   * @return true or false
   */
  private boolean shouldIndex(InputRow inputRow)
  {
    if (getImplicitLockInterval().get().contains(inputRow.getTimestampFromEpoch())) {
      return schema.getShardSpec().isInChunk(inputRow);
    } else {
      return false;
    }
  }

  @JsonProperty("firehose")
  public FirehoseFactory getFirehoseFactory()
  {
    return firehoseFactory;
  }

  @JsonProperty
  public Schema getSchema()
  {
    return schema;
  }

  @JsonProperty
  public int getRowFlushBoundary()
  {
    return rowFlushBoundary;
  }
}
