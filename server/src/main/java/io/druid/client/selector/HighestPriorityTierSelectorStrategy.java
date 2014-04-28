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

package io.druid.client.selector;

import com.google.inject.Inject;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 */
public class HighestPriorityTierSelectorStrategy extends AbstractTierSelectorStrategy
{
  @Inject
  public HighestPriorityTierSelectorStrategy(ServerSelectorStrategy serverSelectorStrategy)
  {
    super(serverSelectorStrategy);
  }

  @Override
  public Map.Entry<Integer, Set<QueryableDruidServer>> getServers(TreeMap<Integer, Set<QueryableDruidServer>> prioritizedServers)
  {
    return prioritizedServers.pollLastEntry();
  }
}
