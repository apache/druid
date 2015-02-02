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
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import io.druid.tasklogs.TaskLogStreamer;

import java.io.IOException;
import java.util.List;

/**
 * Provides task logs based on a series of underlying task log providers.
 */
public class SwitchingTaskLogStreamer implements TaskLogStreamer
{
  private final List<TaskLogStreamer> providers;

  @Inject
  public SwitchingTaskLogStreamer(List<TaskLogStreamer> providers)
  {
    this.providers = ImmutableList.copyOf(providers);
  }

  @Override
  public Optional<ByteSource> streamTaskLog(String taskid, long offset) throws IOException
  {
    for (TaskLogStreamer provider : providers) {
      final Optional<ByteSource> stream = provider.streamTaskLog(taskid, offset);
      if (stream.isPresent()) {
        return stream;
      }
    }

    return Optional.absent();
  }
}
