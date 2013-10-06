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

package io.druid.indexing.overlord;

import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import org.joda.time.Period;

/**
 */
public class TestRemoteTaskRunnerConfig extends RemoteTaskRunnerConfig
{
  @Override
  public boolean isCompressZnodes()
  {
    return false;
  }

  @Override
  public Period getTaskAssignmentTimeout()
  {
    return new Period("PT1S");
  }

  @Override
  public long getMaxZnodeBytes()
  {
    return 1000;
  }

  @Override
  public String getWorkerVersion()
  {
    return "";
  }
}
