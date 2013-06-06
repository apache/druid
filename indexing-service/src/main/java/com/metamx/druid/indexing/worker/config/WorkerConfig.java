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

package com.metamx.druid.indexing.worker.config;

import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class WorkerConfig
{
  @Config("druid.host")
  public abstract String getHost();

  @Config("druid.worker.threads")
  @Default("1")
  public abstract int getNumThreads();

  @Config("druid.worker.ip")
  public abstract String getIp();

  @Config("druid.worker.version")
  public abstract String getVersion();

  @Config("druid.worker.masterService")
  public abstract String getMasterService();

  @Config("druid.worker.capacity")
  public int getCapacity()
  {
    return Runtime.getRuntime().availableProcessors() - 1;
  }
}
