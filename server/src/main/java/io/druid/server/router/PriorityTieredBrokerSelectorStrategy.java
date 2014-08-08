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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import io.druid.query.Query;

/**
 */
public class PriorityTieredBrokerSelectorStrategy implements TieredBrokerSelectorStrategy
{
  private final int minPriority;
  private final int maxPriority;

  @JsonCreator
  public PriorityTieredBrokerSelectorStrategy(
      @JsonProperty("minPriority") Integer minPriority,
      @JsonProperty("maxPriority") Integer maxPriority
  )
  {
    this.minPriority = minPriority == null ? 0 : minPriority;
    this.maxPriority = maxPriority == null ? 1 : maxPriority;
  }

  @Override
  public Optional<String> getBrokerServiceName(TieredBrokerConfig tierConfig, Query query)
  {
    final int priority = query.getContextPriority(0);

    if (priority < minPriority) {
      return Optional.of(
          Iterables.getLast(
              tierConfig.getTierToBrokerMap().values(),
              tierConfig.getDefaultBrokerServiceName()
          )
      );
    } else if (priority >= maxPriority) {
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
