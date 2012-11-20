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

package com.metamx.druid.merger.common.config;

import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class IndexerZkConfig
{
  @Config("druid.zk.paths.indexer.announcementsPath")
  public abstract String getAnnouncementPath();

  @Config("druid.zk.paths.indexer.tasksPath")
  public abstract String getTaskPath();

  @Config("druid.zk.paths.indexer.statusPath")
  public abstract String getStatusPath();

  @Config("druid.zk.maxNumBytes")
  @Default("512000")
  public abstract long getMaxNumBytes();
}
