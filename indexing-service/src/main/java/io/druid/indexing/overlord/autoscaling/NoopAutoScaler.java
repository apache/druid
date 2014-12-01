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

import com.metamx.common.UOE;
import com.metamx.emitter.EmittingLogger;

import java.util.List;

/**
 * This class just logs when scaling should occur.
 */
public class NoopAutoScaler<Void> implements AutoScaler<Void>
{
  private static final EmittingLogger log = new EmittingLogger(NoopAutoScaler.class);

  @Override
  public int getMinNumWorkers()
  {
    return 0;
  }

  @Override
  public int getMaxNumWorkers()
  {
    return 0;
  }

  @Override
  public Void getEnvConfig()
  {
    throw new UOE("No config for Noop!");
  }

  @Override
  public AutoScalingData provision()
  {
    log.info("If I were a real strategy I'd create something now");
    return null;
  }

  @Override
  public AutoScalingData terminate(List<String> ips)
  {
    log.info("If I were a real strategy I'd terminate %s now", ips);
    return null;
  }

  @Override
  public AutoScalingData terminateWithIds(List<String> ids)
  {
    log.info("If I were a real strategy I'd terminate %s now", ids);
    return null;
  }

  @Override
  public List<String> ipToIdLookup(List<String> ips)
  {
    log.info("I'm not a real strategy so I'm returning what I got %s", ips);
    return ips;
  }

  @Override
  public List<String> idToIpLookup(List<String> nodeIds)
  {
    log.info("I'm not a real strategy so I'm returning what I got %s", nodeIds);
    return nodeIds;
  }
}
