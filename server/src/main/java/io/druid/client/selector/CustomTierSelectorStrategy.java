/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.client.selector;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;

import java.util.Comparator;
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

    final Map<Integer, Integer> lookup = Maps.newHashMap();
    int pos = 0;
    for (Integer integer : config.getPriorities()) {
      lookup.put(integer, pos);
      pos++;
    }

    this.comparator = new Comparator<Integer>()
    {
      @Override
      public int compare(Integer o1, Integer o2)
      {
        int pos1 = lookup.get(o1);
        int pos2 = lookup.get(o2);
        return Ints.compare(pos1, pos2);
      }
    };
  }

  @Override
  public Comparator<Integer> getComparator()
  {
    return comparator;
  }
}
