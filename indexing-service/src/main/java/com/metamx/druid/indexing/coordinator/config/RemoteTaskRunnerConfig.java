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

package com.metamx.druid.indexing.coordinator.config;

import com.metamx.druid.indexing.common.config.IndexerZkConfig;
import org.joda.time.Duration;
import org.skife.config.Config;
import org.skife.config.Default;
import org.skife.config.DefaultNull;

/**
 */
public abstract class RemoteTaskRunnerConfig extends IndexerZkConfig
{
  @Config("druid.indexer.taskAssignmentTimeoutDuration")
  @Default("PT5M")
  public abstract Duration getTaskAssignmentTimeoutDuration();

  @Config("druid.curator.compression.enable")
  @Default("false")
  public abstract boolean enableCompression();

  @Config("druid.indexer.worker.version")
  @DefaultNull
  public abstract String getWorkerVersion();
}
