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

package io.druid.query.search;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.java.util.common.ISE;

public class SearchStrategySelector
{
  private static final EmittingLogger log = new EmittingLogger(SearchStrategySelector.class);
  private final SearchQueryConfig config;

  @Inject
  public SearchStrategySelector(Supplier<SearchQueryConfig> configSupplier)
  {
    this.config = configSupplier.get();
  }

  public SearchStrategy strategize(SearchQuery query)
  {
    final String strategyString = config.withOverrides(query).getSearchStrategy();

    switch (strategyString) {
      case AutoStrategy.NAME:
        log.debug("Auto strategy is selected, query id [%s]", query.getId());
        return AutoStrategy.of(query);
      case UseIndexesStrategy.NAME:
        log.debug("Use-index strategy is selected, query id [%s]", query.getId());
        return UseIndexesStrategy.of(query);
      case CursorOnlyStrategy.NAME:
        log.debug("Cursor-only strategy is selected, query id [%s]", query.getId());
        return CursorOnlyStrategy.of(query);
      default:
        throw new ISE("Unknown strategy[%s], query id [%s]", strategyString, query.getId());
    }
  }
}
