/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.client.selector;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class CustomTierSelectorStrategy extends AbstractTierSelectorStrategy
{
  private final Comparator<Integer> comparator;

  @JsonCreator
  public CustomTierSelectorStrategy(
      @JacksonInject ServerSelectorStrategy serverSelectorStrategy,
      @JacksonInject CustomTierSelectorStrategyConfig config
  )
  {
    super(serverSelectorStrategy);

    final Map<Integer, Integer> lookup = new HashMap<>();
    int pos = 0;
    for (Integer integer : config.getPriorities()) {
      lookup.put(integer, pos);
      pos++;
    }

    // Tiers with priorities explicitly specified in the custom priority list config always have higher priority than
    // those not and those not specified fall back to use the highest priority strategy among themselves
    this.comparator = (p1, p2) -> {
      final Integer rank1 = lookup.get(p1);
      final Integer rank2 = lookup.get(p2);

      if (rank1 != null && rank2 != null) {
        return Integer.compare(rank1, rank2);
      }
      if (rank1 != null) {
        return -1;
      }
      if (rank2 != null) {
        return 1;
      }

      // Fall back to highest priority first
      return Integer.compare(p2, p1);
    };
  }

  @Override
  public Comparator<Integer> getComparator()
  {
    return comparator;
  }
}
