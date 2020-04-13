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

package org.apache.druid.server.router;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;

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
    final int priority = QueryContexts.getPriority(query);

    if (priority < minPriority || priority > maxPriority) {
      return Optional.of(
          tierConfig.getDefaultBrokerServiceName()
      );
    }

    return Optional.absent();
  }
}
