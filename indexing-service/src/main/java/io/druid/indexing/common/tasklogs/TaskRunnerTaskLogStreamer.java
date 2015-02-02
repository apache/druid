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

package io.druid.indexing.common.tasklogs;

import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.tasklogs.TaskLogStreamer;

import java.io.IOException;

/**
*/
public class TaskRunnerTaskLogStreamer implements TaskLogStreamer
{
  private final TaskMaster taskMaster;

  @Inject
  public TaskRunnerTaskLogStreamer(
      final TaskMaster taskMaster
  ) {
    this.taskMaster = taskMaster;
  }

  @Override
  public Optional<ByteSource> streamTaskLog(String taskid, long offset) throws IOException
  {
    final TaskRunner runner = taskMaster.getTaskRunner().orNull();
    if (runner instanceof TaskLogStreamer) {
      return ((TaskLogStreamer) runner).streamTaskLog(taskid, offset);
    } else {
      return Optional.absent();
    }
  }
}
