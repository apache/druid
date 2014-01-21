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

package io.druid.indexing.overlord.scaling;

import java.util.List;

/**
 * The AutoScalingStrategy has the actual methods to provision and terminate worker nodes.
 */
public interface AutoScalingStrategy
{
  public AutoScalingData provision();

  public AutoScalingData terminate(List<String> ips);

  public AutoScalingData terminateWithIds(List<String> ids);

  /**
   * Provides a lookup of ip addresses to node ids
   * @param ips - nodes IPs
   * @return node ids
   */
  public List<String> ipToIdLookup(List<String> ips);

  /**
   * Provides a lookup of node ids to ip addresses
   * @param nodeIds - nodes ids
   * @return IPs associated with the node
   */
  public List<String> idToIpLookup(List<String> nodeIds);
}
