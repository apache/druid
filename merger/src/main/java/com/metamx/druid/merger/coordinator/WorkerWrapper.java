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

package com.metamx.druid.merger.coordinator;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.worker.Worker;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

/**
 */
public class WorkerWrapper implements Closeable
{
  private final Worker worker;
  private final PathChildrenCache statusCache;
  private final Function<ChildData, String> cacheConverter;

  private volatile DateTime lastCompletedTaskTime = new DateTime();

  public WorkerWrapper(Worker worker, PathChildrenCache statusCache, final ObjectMapper jsonMapper)
  {
    this.worker = worker;
    this.statusCache = statusCache;
    this.cacheConverter = new Function<ChildData, String>()
    {
      @Override
      public String apply(@Nullable ChildData input)
      {
        try {
          return jsonMapper.readValue(input.getData(), TaskStatus.class).getId();
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  public Worker getWorker()
  {
    return worker;
  }

  public Set<String> getRunningTasks()
  {
    return Sets.newHashSet(
        Lists.transform(
            statusCache.getCurrentData(),
            cacheConverter
        )
    );
  }

  public PathChildrenCache getStatusCache()
  {
    return statusCache;
  }

  public DateTime getLastCompletedTaskTime()
  {
    return lastCompletedTaskTime;
  }

  public boolean isAtCapacity()
  {
    return statusCache.getCurrentData().size() >= worker.getCapacity();
  }

  public void setLastCompletedTaskTime(DateTime completedTaskTime)
  {
    lastCompletedTaskTime = completedTaskTime;
  }

  @Override
  public void close() throws IOException
  {
    statusCache.close();
  }
}
