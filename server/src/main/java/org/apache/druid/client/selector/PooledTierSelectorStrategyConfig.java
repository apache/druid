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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.DruidException;
import org.apache.druid.utils.CollectionUtils;

import java.util.Set;

/**
 * Configuration for {@link PooledTierSelectorStrategy}.
 * <p>
 * Requires a non-empty set of {@code priorities}. The order of priorities don't matter.
 */
public class PooledTierSelectorStrategyConfig
{
  @JsonProperty
  private final Set<Integer> priorities;

  public Set<Integer> getPriorities()
  {
    return priorities;
  }

  public PooledTierSelectorStrategyConfig(@JsonProperty("priorities") final Set<Integer> priorities)
  {
    if (CollectionUtils.isNullOrEmpty(priorities)) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "priorities must be non-empty when using pooled tier selector on the Broker. Found priorities[%s].",
                              priorities
                          );
    }
    this.priorities = priorities;
  }

  @Override
  public String toString()
  {
    return "PooledTierSelectorStrategyConfig{" +
           "priorities=" + priorities +
           '}';
  }
}
