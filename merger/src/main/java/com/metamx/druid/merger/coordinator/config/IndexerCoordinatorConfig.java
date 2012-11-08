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

package com.metamx.druid.merger.coordinator.config;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.metamx.druid.merger.common.task.Task;
import org.skife.config.Config;
import org.skife.config.Default;

import java.io.File;
import java.util.Set;

/**
 */
public abstract class IndexerCoordinatorConfig
{
  private volatile Set<String> whitelistDatasources = null;

  @Config("druid.host")
  public abstract String getServerName();

  @Config("druid.zk.paths.indexer.leaderLatchPath")
  public abstract String getLeaderLatchPath();

  @Config("druid.merger.threads")
  @Default("1")
  public abstract int getNumLocalThreads();

  @Config("druid.merger.runner")
  @Default("remote")
  public abstract String getRunnerImpl();

  @Config("druid.merger.storage")
  @Default("local")
  public abstract String getStorageImpl();

  @Config("druid.merger.taskDir")
  public abstract File getBaseTaskDir();

  @Config("druid.merger.whitelist.enabled")
  @Default("false")
  public abstract boolean isWhitelistEnabled();

  @Config("druid.merger.whitelist.datasources")
  @Default("")
  public abstract String getWhitelistDatasourcesString();

  public File getTaskDir(final Task task) {
    return new File(getBaseTaskDir(), task.getId());
  }

  public Set<String> getWhitelistDatasources()
  {
    if(whitelistDatasources == null) {
      synchronized (this) {
        if(whitelistDatasources == null) {
          whitelistDatasources = ImmutableSet.copyOf(Splitter.on(",").split(getWhitelistDatasourcesString()));
        }
      }
    }

    return whitelistDatasources;
  }

  @Config("druid.merger.rowFlushBoundary")
  @Default("500000")
  public abstract long getRowFlushBoundary();

  @Config("druid.indexer.strategy")
  @Default("noop")
  public abstract String getStrategyImpl();
}
