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

package io.druid.server.metrics;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.metamx.metrics.SysMonitor;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.StorageLocationConfig;

import java.util.List;

/**
 */
public class DruidSysMonitor extends SysMonitor
{
  @Inject
  public DruidSysMonitor(
      SegmentLoaderConfig config
  )
  {
    final List<StorageLocationConfig> locs = config.getLocations();
    List<String> dirs = Lists.newArrayListWithExpectedSize(locs.size());
    for (StorageLocationConfig loc : locs) {
      dirs.add(loc.getPath().toString());
    }

    addDirectoriesToMonitor(dirs.toArray(new String[dirs.size()]));
  }
}
