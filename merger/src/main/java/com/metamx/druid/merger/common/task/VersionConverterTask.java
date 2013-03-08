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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.ISE;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.druid.loading.SegmentLoadingException;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.actions.SegmentInsertAction;
import com.metamx.druid.merger.common.actions.SpawnTasksAction;
import com.metamx.druid.merger.common.actions.TaskActionClient;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class VersionConverterTask extends AbstractTask
{
  private static final String TYPE = "version_converter";
  private static final Integer CURR_VERSION_INTEGER = new Integer(IndexIO.CURRENT_VERSION_ID);

  private static final Logger log = new Logger(VersionConverterTask.class);
  private final DataSegment segment;

  public VersionConverterTask(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("segment") DataSegment segment
  )
  {
    super(
        joinId(TYPE, dataSource, interval.getStart(), interval.getEnd(), new DateTime()),
        dataSource,
        interval
    );

    this.segment = segment;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    if (segment == null) {
      throw new ISE("Segment was null, this should never run.", this.getClass().getSimpleName());
    }

    log.info("I'm in a subless mood.");
    convertSegment(toolbox, segment);
    return success();
  }

  @Override
  public TaskStatus preflight(TaskToolbox toolbox) throws Exception
  {
    if (segment != null) {
      return super.preflight(toolbox);
    }

    final TaskActionClient taskClient = toolbox.getTaskActionClient();

    List<DataSegment> segments = taskClient.submit(defaultListUsedAction());

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

    taskClient.submit(new SpawnTasksAction(Lists.newArrayList(tasks)));

    return TaskStatus.success(getId());
  }

  public static class SubTask extends AbstractTask
  {
    private final DataSegment segment;

    protected SubTask(
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
    final Map<DataSegment, File> localSegments = toolbox.getSegments(Arrays.asList(segment));

    final File location = localSegments.get(segment);
    final File outLocation = new File(location, "v9_out");
    if (IndexIO.convertSegment(location, outLocation)) {
      final int outVersion = IndexIO.getVersionFromDir(outLocation);

      // Appending to the version makes a new version that inherits most comparability parameters of the original
      // version, but is "newer" than said original version.
      DataSegment updatedSegment = segment.withVersion(String.format("%s_v%s", segment.getVersion(), outVersion));
      updatedSegment = toolbox.getSegmentPusher().push(outLocation, updatedSegment);

      toolbox.getTaskActionClient().submit(new SegmentInsertAction(Sets.newHashSet(updatedSegment)));
    } else {
      log.info("Conversion failed.");
    }
  }
}
