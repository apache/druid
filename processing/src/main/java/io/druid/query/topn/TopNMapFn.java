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

package io.druid.query.topn;

import com.google.common.base.Function;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.Result;
import io.druid.query.topn.types.TopNStrategyFactory;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionHandlerUtils;

public class TopNMapFn implements Function<Cursor, Result<TopNResultValue>>
{
  private static final TopNStrategyFactory STRATEGY_FACTORY = new TopNStrategyFactory();

  private final TopNQuery query;
  private final TopNAlgorithm topNAlgorithm;

  public TopNMapFn(
      TopNQuery query,
      TopNAlgorithm topNAlgorithm
  )
  {
    this.query = query;
    this.topNAlgorithm = topNAlgorithm;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Result<TopNResultValue> apply(Cursor cursor)
  {
    final ColumnSelectorPlus selectorPlus = DimensionHandlerUtils.createColumnSelectorPlus(
        STRATEGY_FACTORY,
        query.getDimensionSpec(),
        cursor
    );

    if (selectorPlus.getSelector() == null) {
      return null;
    }

    TopNParams params = null;
    try {
      params = topNAlgorithm.makeInitParams(selectorPlus, cursor);

      TopNResultBuilder resultBuilder = BaseTopNAlgorithm.makeResultBuilder(params, query);

      topNAlgorithm.run(params, resultBuilder, null);

      return resultBuilder.build();
    }
    finally {
      topNAlgorithm.cleanup(params);
    }
  }
}
