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

package com.metamx.druid.merger.coordinator.scaling;

import com.metamx.emitter.EmittingLogger;

import java.util.List;

/**
 * This class just logs when scaling should occur.
 */
public class NoopScalingStrategy implements ScalingStrategy<String>
{
  private static final EmittingLogger log = new EmittingLogger(NoopScalingStrategy.class);

  @Override
  public AutoScalingData<String> provision()
  {
    log.info("If I were a real strategy I'd create something now");
    return null;
  }

  @Override
  public AutoScalingData<String> terminate(List<String> nodeIds)
  {
    log.info("If I were a real strategy I'd terminate %s now", nodeIds);
    return null;
  }

  @Override
  public List<String> ipLookup(List<String> ips)
  {
    log.info("I'm not a real strategy so I'm returning what I got %s", ips);
    return ips;
  }
}
