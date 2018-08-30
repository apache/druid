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

package org.apache.druid.query.topn;

import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.Result;
import org.apache.druid.query.topn.types.TopNColumnSelectorStrategyFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionHandlerUtils;

import javax.annotation.Nullable;

public class TopNMapFn
{
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

  @SuppressWarnings("unchecked")
  public Result<TopNResultValue> apply(final Cursor cursor, final @Nullable TopNQueryMetrics queryMetrics)
  {
    final ColumnSelectorPlus selectorPlus = DimensionHandlerUtils.createColumnSelectorPlus(
        new TopNColumnSelectorStrategyFactory(query.getDimensionSpec().getOutputType()),
        query.getDimensionSpec(),
        cursor.getColumnSelectorFactory()
    );

    if (selectorPlus.getSelector() == null) {
      return null;
    }

    TopNParams params = null;
    try {
      params = topNAlgorithm.makeInitParams(selectorPlus, cursor);
      if (queryMetrics != null) {
        queryMetrics.columnValueSelector(selectorPlus.getSelector());
        queryMetrics.numValuesPerPass(params);
      }

      TopNResultBuilder resultBuilder = BaseTopNAlgorithm.makeResultBuilder(params, query);

      topNAlgorithm.run(params, resultBuilder, null, queryMetrics);

      return resultBuilder.build();
    }
    finally {
      topNAlgorithm.cleanup(params);
    }
  }
}
