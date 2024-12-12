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

package org.apache.druid.query.groupby.epinephelinae;

import org.apache.druid.java.util.common.granularity.AllGranularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.BinaryOperator;

/**
 * Class that knows how to merge aggregator data from two groupBy {@link ResultRow} objects that have the same time
 * and dimensions. This code runs on Brokers as well as data servers, like Historicals.
 *
 * Used by
 * {@link org.apache.druid.query.groupby.GroupingEngine#mergeResults}.
 */
public class GroupByResultMergeFn implements BinaryOperator<ResultRow>
{
  private final GroupByQuery query;

  public GroupByResultMergeFn(GroupByQuery query)
  {
    this.query = query;
  }

  @Override
  @Nullable
  public ResultRow apply(@Nullable final ResultRow arg1, @Nullable final ResultRow arg2)
  {
    if (arg1 == null) {
      return arg2;
    } else if (arg2 == null) {
      return arg1;
    }

    final ResultRow newResult = ResultRow.create(query.getResultRowSizeWithoutPostAggregators());

    // Add timestamp.
    if (query.getResultRowHasTimestamp()) {
      newResult.set(0, adjustTimestamp(arg1));
    }

    // Add dimensions.
    final int dimensionStart = query.getResultRowDimensionStart();
    final List<DimensionSpec> dimensions = query.getDimensions();
    for (int i = 0; i < dimensions.size(); i++) {
      final int rowIndex = dimensionStart + i;
      newResult.set(rowIndex, arg1.get(rowIndex));
    }

    // Add aggregations.
    final int aggregatorStart = query.getResultRowAggregatorStart();
    final List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();
    for (int i = 0; i < aggregatorSpecs.size(); i++) {
      final AggregatorFactory aggregatorFactory = aggregatorSpecs.get(i);
      final int rowIndex = aggregatorStart + i;
      newResult.set(rowIndex, aggregatorFactory.combine(arg1.get(rowIndex), arg2.get(rowIndex)));
    }

    return newResult;
  }

  private long adjustTimestamp(final ResultRow row)
  {
    if (query.getGranularity() instanceof AllGranularity) {
      return row.getLong(0);
    } else {
      return query.getGranularity().bucketStart(row.getLong(0));
    }
  }
}
