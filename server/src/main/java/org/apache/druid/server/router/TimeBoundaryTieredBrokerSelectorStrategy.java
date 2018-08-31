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

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import org.apache.druid.query.Query;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;

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
