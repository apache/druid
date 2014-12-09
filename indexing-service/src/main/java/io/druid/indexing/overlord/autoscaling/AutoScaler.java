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

package io.druid.indexing.overlord.autoscaling;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.indexing.overlord.autoscaling.ec2.EC2AutoScaler;

import java.util.List;

/**
 * The AutoScaler has the actual methods to provision and terminate worker nodes.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = NoopAutoScaler.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "ec2", value = EC2AutoScaler.class)
})
public interface AutoScaler<T>
{
  public int getMinNumWorkers();

  public int getMaxNumWorkers();

  public T getEnvConfig();

  public AutoScalingData provision();

  public AutoScalingData terminate(List<String> ips);

  public AutoScalingData terminateWithIds(List<String> ids);

  /**
   * Provides a lookup of ip addresses to node ids
   *
   * @param ips - nodes IPs
   *
   * @return node ids
   */
  public List<String> ipToIdLookup(List<String> ips);

  /**
   * Provides a lookup of node ids to ip addresses
   *
   * @param nodeIds - nodes ids
   *
   * @return IPs associated with the node
   */
  public List<String> idToIpLookup(List<String> nodeIds);
}
