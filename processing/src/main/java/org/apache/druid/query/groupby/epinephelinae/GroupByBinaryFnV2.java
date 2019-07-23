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

import com.google.common.collect.Maps;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.granularity.AllGranularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.joda.time.DateTime;

import java.util.Map;
import java.util.function.BinaryOperator;

public class GroupByBinaryFnV2 implements BinaryOperator<Row>
{
  private final GroupByQuery query;

  public GroupByBinaryFnV2(GroupByQuery query)
  {
    this.query = query;
  }

  @Override
  public Row apply(final Row arg1, final Row arg2)
  {
    if (arg1 == null) {
      return arg2;
    } else if (arg2 == null) {
      return arg1;
    }

    final Map<String, Object> newMap = Maps.newHashMapWithExpectedSize(
        query.getDimensions().size() + query.getAggregatorSpecs().size()
    );

    // Add dimensions
    for (DimensionSpec dimension : query.getDimensions()) {
      newMap.put(dimension.getOutputName(), arg1.getRaw(dimension.getOutputName()));
    }

    // Add aggregations
    for (AggregatorFactory aggregatorFactory : query.getAggregatorSpecs()) {
      newMap.put(
          aggregatorFactory.getName(),
          aggregatorFactory.combine(
              arg1.getRaw(aggregatorFactory.getName()),
              arg2.getRaw(aggregatorFactory.getName())
          )
      );
    }

    return new MapBasedRow(adjustTimestamp(arg1), newMap);
  }

  private DateTime adjustTimestamp(final Row row)
  {
    if (query.getGranularity() instanceof AllGranularity) {
      return row.getTimestamp();
    } else {
      return query.getGranularity().bucketStart(row.getTimestamp());
    }
  }
}
