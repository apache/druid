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

package com.metamx.druid.master.rules;

import com.metamx.druid.client.DataSegment;
import com.metamx.druid.master.DruidMaster;
import com.metamx.druid.master.DruidMasterRuntimeParams;
import com.metamx.druid.master.MasterStats;

/**
 * DropRules indicate when segments should be completely removed from the cluster.
 */
public abstract class DropRule implements Rule
{
  @Override
  public MasterStats run(DruidMaster master, DruidMasterRuntimeParams params, DataSegment segment)
  {
    MasterStats stats = new MasterStats();
    master.removeSegment(segment);
    stats.addToGlobalStat("deletedCount", 1);
    return stats;
  }
}
