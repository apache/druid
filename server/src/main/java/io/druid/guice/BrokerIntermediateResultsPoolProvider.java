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

package io.druid.guice;

import io.druid.java.util.common.ISE;
import io.druid.query.DruidProcessingConfig;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.strategy.GroupByStrategySelector;

import javax.inject.Inject;

@LazySingleton
public class BrokerIntermediateResultsPoolProvider extends IntermediateResultsPoolProvider
{
  @Inject
  public BrokerIntermediateResultsPoolProvider(
      DruidProcessingConfig processingConfig,
      GroupByQueryConfig groupByQueryConfig
  )
  {
    super(choosePoolSize(processingConfig, groupByQueryConfig), processingConfig);
  }

  private static int choosePoolSize(DruidProcessingConfig processingConfig, GroupByQueryConfig groupByQueryConfig)
  {
    switch (groupByQueryConfig.getDefaultStrategy()) {
      case GroupByStrategySelector.STRATEGY_V1:
        return processingConfig.getNumThreads();
      case GroupByStrategySelector.STRATEGY_V2:
        return 0;
      default:
        throw new ISE("Unknown default groupBy strategy[%s]", groupByQueryConfig.getDefaultStrategy());
    }
  }
}
