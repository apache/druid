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
import com.metamx.common.ISE;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.actions.SpawnTasksAction;
import com.metamx.druid.merger.common.actions.TaskActionClient;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;

/**
 */
public class VersionConverterTask extends AbstractTask
{
  private static final String TYPE = "version_converter";
  private static final Integer CURR_VERSION_INTEGER = new Integer(IndexIO.CURRENT_VERSION_ID);

  private static final Logger log = new Logger(VersionConverterTask.class);

  public VersionConverterTask(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval
  )
  {
    super(
        joinId(TYPE, dataSource, interval.getStart(), interval.getEnd(), new DateTime()),
        dataSource,
        interval
    );
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    throw new ISE("Should never run, %s just exists to create subtasks", this.getClass().getSimpleName());
  }

  @Override
  public TaskStatus preflight(TaskToolbox toolbox) throws Exception
  {
    final TaskActionClient taskClient = toolbox.getTaskActionClientFactory();

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
                  return new VersionConverterSubTask(getGroupId(), segment);
                }

                log.info("Skipping[%s], already version[%s]", segment.getIdentifier(), segmentVersion);
                return null;
              }
            }
        );

    taskClient.submit(new SpawnTasksAction(Lists.newArrayList(tasks)));

    return TaskStatus.success(getId());
  }
}
