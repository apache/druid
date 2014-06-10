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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.logger.Logger;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentInsertAction;
import io.druid.indexing.common.actions.SegmentListUsedAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.segment.IndexIO;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class VersionConverterTask extends AbstractFixedIntervalTask
{
  private static final String TYPE = "version_converter";
  private static final Integer CURR_VERSION_INTEGER = IndexIO.CURRENT_VERSION_ID;

  private static final Logger log = new Logger(VersionConverterTask.class);

  @JsonIgnore
  private final DataSegment segment;

  public static VersionConverterTask create(String dataSource, Interval interval)
  {
    final String id = makeId(dataSource, interval);
    return new VersionConverterTask(id, id, dataSource, interval, null);
  }

  public static VersionConverterTask create(DataSegment segment)
  {
    final Interval interval = segment.getInterval();
    final String dataSource = segment.getDataSource();
    final String id = makeId(dataSource, interval);
    return new VersionConverterTask(id, id, dataSource, interval, segment);
  }

  private static String makeId(String dataSource, Interval interval)
  {
    Preconditions.checkNotNull(dataSource, "dataSource");
    Preconditions.checkNotNull(interval, "interval");
    return joinId(TYPE, dataSource, interval.getStart(), interval.getEnd(), new DateTime());
  }

  @JsonCreator
  private static VersionConverterTask createFromJson(
      @JsonProperty("id") String id,
      @JsonProperty("groupId") String groupId,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("segment") DataSegment segment
  )
  {
    if (id == null) {
      if (segment == null) {
        return create(dataSource, interval);
      } else {
        return create(segment);
      }
    }
    return new VersionConverterTask(id, groupId, dataSource, interval, segment);
  }

  private VersionConverterTask(
      String id,
      String groupId,
      String dataSource,
      Interval interval,
      DataSegment segment
  )
  {
    super(id, groupId, dataSource, interval);
    this.segment = segment;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @JsonProperty
  public DataSegment getSegment()
  {
    return segment;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    if (segment == null) {
      final List<DataSegment> segments = toolbox.getTaskActionClient().submit(
          new SegmentListUsedAction(
              getDataSource(),
              getInterval()
          )
      );
      final FunctionalIterable<Task> tasks = FunctionalIterable
          .create(segments)
          .keep(
              new Function<DataSegment, Task>()
              {
                @Override
                public Task apply(DataSegment segment)
                {
                  final Integer segmentVersion = segment.getBinaryVersion();
                  if (!CURR_VERSION_INTEGER.equals(segmentVersion)) {
                    return new SubTask(getGroupId(), segment);
                  }

                  log.info("Skipping[%s], already version[%s]", segment.getIdentifier(), segmentVersion);
                  return null;
                }
              }
          );

      // Vestigial from a past time when this task spawned subtasks.
      for (final Task subTask : tasks) {
        final TaskStatus status = subTask.run(toolbox);
        if (!status.isSuccess()) {
          return status;
        }
      }
    } else {
      log.info("I'm in a subless mood.");
      convertSegment(toolbox, segment);
    }
    return success();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    VersionConverterTask that = (VersionConverterTask) o;

    if (segment != null ? !segment.equals(that.segment) : that.segment != null) {
      return false;
    }

    return super.equals(o);
  }

  public static class SubTask extends AbstractFixedIntervalTask
  {
    @JsonIgnore
    private final DataSegment segment;

    @JsonCreator
    public SubTask(
        @JsonProperty("groupId") String groupId,
        @JsonProperty("segment") DataSegment segment
    )
    {
      super(
          joinId(
              groupId,
              "sub",
              segment.getInterval().getStart(),
              segment.getInterval().getEnd(),
              segment.getShardSpec().getPartitionNum()
          ),
          groupId,
          segment.getDataSource(),
          segment.getInterval()
      );
      this.segment = segment;
    }

    @JsonProperty
    public DataSegment getSegment()
    {
      return segment;
    }

    @Override
    public String getType()
    {
      return "version_converter_sub";
    }

    @Override
    public TaskStatus run(TaskToolbox toolbox) throws Exception
    {
      log.info("Subs are good!  Italian BMT and Meatball are probably my favorite.");
      convertSegment(toolbox, segment);
      return success();
    }
  }

  private static void convertSegment(TaskToolbox toolbox, final DataSegment segment)
      throws SegmentLoadingException, IOException
  {
    log.info("Converting segment[%s]", segment);
    final TaskActionClient actionClient = toolbox.getTaskActionClient();
    final List<DataSegment> currentSegments = actionClient.submit(
        new SegmentListUsedAction(segment.getDataSource(), segment.getInterval())
    );

    for (DataSegment currentSegment : currentSegments) {
      final String version = currentSegment.getVersion();
      final Integer binaryVersion = currentSegment.getBinaryVersion();

      if (version.startsWith(segment.getVersion()) && CURR_VERSION_INTEGER.equals(binaryVersion)) {
        log.info("Skipping already updated segment[%s].", segment);
        return;
      }
    }

    final Map<DataSegment, File> localSegments = toolbox.fetchSegments(Arrays.asList(segment));

    final File location = localSegments.get(segment);
    final File outLocation = new File(location, "v9_out");
    if (IndexIO.convertSegment(location, outLocation, toolbox.getColumnConfig())) {
      final int outVersion = IndexIO.getVersionFromDir(outLocation);

      // Appending to the version makes a new version that inherits most comparability parameters of the original
      // version, but is "newer" than said original version.
      DataSegment updatedSegment = segment.withVersion(String.format("%s_v%s", segment.getVersion(), outVersion));
      updatedSegment = toolbox.getSegmentPusher().push(outLocation, updatedSegment);

      actionClient.submit(new SegmentInsertAction(Sets.newHashSet(updatedSegment)).withAllowOlderVersions(true));
    } else {
      log.info("Conversion failed.");
    }
  }
}
