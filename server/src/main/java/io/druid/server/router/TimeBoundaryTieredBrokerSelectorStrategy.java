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

package io.druid.server.router;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import io.druid.query.Query;
import io.druid.query.timeboundary.TimeBoundaryQuery;

/**
 */
public class TimeBoundaryTieredBrokerSelectorStrategy implements TieredBrokerSelectorStrategy
{
  @Override
  public Optional<String> getBrokerServiceName(TieredBrokerConfig tierConfig, Query query)
  {
    // Somewhat janky way of always selecting highest priority broker for this type of query
    if (query instanceof TimeBoundaryQuery) {
      return Optional.of(
          Iterables.getFirst(
              tierConfig.getTierToBrokerMap().values(),
              tierConfig.getDefaultBrokerServiceName()
          )
      );
    }

    return Optional.absent();
  }
}
