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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.index.YeOldePlumberSchool;
import com.metamx.druid.merger.coordinator.TaskContext;
import com.metamx.druid.realtime.FireDepartmentMetrics;
import com.metamx.druid.realtime.Firehose;
import com.metamx.druid.realtime.FirehoseFactory;
import com.metamx.druid.realtime.Plumber;
import com.metamx.druid.realtime.Schema;
import com.metamx.druid.loading.SegmentPusher;
import com.metamx.druid.realtime.Sink;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class IndexGeneratorTask extends AbstractTask
{
  @JsonProperty
  private final FirehoseFactory firehoseFactory;

  @JsonProperty
  private final Schema schema;

  private static final Logger log = new Logger(IndexTask.class);

  @JsonCreator
  public IndexGeneratorTask(
      @JsonProperty("groupId") String groupId,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("firehose") FirehoseFactory firehoseFactory,
      @JsonProperty("schema") Schema schema
  )
  {
    super(
        String.format(
            "%s_generator_%s_%s_%s",
            groupId,
            interval.getStart(),
            interval.getEnd(),
            schema.getShardSpec().getPartitionNum()
        ),
        groupId,
        schema.getDataSource(),
        interval
    );

    this.firehoseFactory = firehoseFactory;
    this.schema = schema;
  }

  @Override
  public Type getType()
  {
    return Type.INDEX;
  }

  @Override
  public TaskStatus run(final TaskContext context, final TaskToolbox toolbox) throws Exception
  {
    // Set up temporary directory for indexing
    final File tmpDir = new File(
        String.format(
            "%s/%s",
            toolbox.getConfig().getTaskDir(this).toString(),
            String.format(
                "%s_%s_%s_%s_%s",
                this.getDataSource(),
                this.getInterval().getStart(),
                this.getInterval().getEnd(),
                context.getVersion(),
                schema.getShardSpec().getPartitionNum()
            )
        )
    );

    // We need to track published segments.
    final List<DataSegment> pushedSegments = new CopyOnWriteArrayList<DataSegment>();
    final SegmentPusher wrappedSegmentPusher = new SegmentPusher()
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
        getInterval(),
        context.getVersion(),
        wrappedSegmentPusher,
        tmpDir
    ).findPlumber(schema, metrics);

    try {
      while(firehose.hasMore()) {
        final InputRow inputRow = firehose.nextRow();

        if(shouldIndex(inputRow)) {
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

          if(numRows >= toolbox.getConfig().getRowFlushBoundary()) {
            plumber.persist(firehose.commit());
          }
        } else {
          metrics.incrementThrownAway();
        }
      }
    } finally {
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

    // Done
    return TaskStatus.success(getId(), ImmutableList.copyOf(pushedSegments));
  }

  /**
   * Should we index this inputRow? Decision is based on our interval and shardSpec.
   * @param inputRow the row to check
   * @return true or false
   */
  private boolean shouldIndex(InputRow inputRow) {
    if(!getInterval().contains(inputRow.getTimestampFromEpoch())) {
      return false;
    }

    final Map<String, String> eventDimensions = Maps.newHashMapWithExpectedSize(inputRow.getDimensions().size());
    for(final String dim : inputRow.getDimensions()) {
      final List<String> dimValues = inputRow.getDimension(dim);
      if(dimValues.size() == 1) {
        eventDimensions.put(dim, Iterables.getOnlyElement(dimValues));
      }
    }

    return schema.getShardSpec().isInChunk(eventDimensions);
  }
}
