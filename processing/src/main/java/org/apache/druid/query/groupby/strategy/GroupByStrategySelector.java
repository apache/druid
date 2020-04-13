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

package org.apache.druid.query.groupby.strategy;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;

public class GroupByStrategySelector
{
  public static final String STRATEGY_V2 = "v2";
  public static final String STRATEGY_V1 = "v1";

  private final GroupByQueryConfig config;
  private final GroupByStrategyV1 strategyV1;
  private final GroupByStrategyV2 strategyV2;

  @Inject
  public GroupByStrategySelector(
      Supplier<GroupByQueryConfig> configSupplier,
      GroupByStrategyV1 strategyV1,
      GroupByStrategyV2 strategyV2
  )
  {
    this.config = configSupplier.get();
    this.strategyV1 = strategyV1;
    this.strategyV2 = strategyV2;
  }

  public GroupByStrategy strategize(GroupByQuery query)
  {
    final String strategyString = config.withOverrides(query).getDefaultStrategy();

    switch (strategyString) {
      case STRATEGY_V2:
        return strategyV2;

      case STRATEGY_V1:
        // Fail early if subtotals were asked from GroupBy V1
        if (query.getSubtotalsSpec() != null) {
          throw new IAE("GroupBy Strategy [%s] does not support subtotalsSpec.", STRATEGY_V1);
        }

        return strategyV1;

      default:
        throw new ISE("No such strategy[%s]", strategyString);
    }
  }
}
