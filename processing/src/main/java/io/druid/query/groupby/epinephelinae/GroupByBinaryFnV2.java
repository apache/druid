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

package io.druid.query.groupby.epinephelinae;

import com.google.common.collect.Maps;
import com.metamx.common.guava.nary.BinaryFn;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.AllGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class GroupByBinaryFnV2 implements BinaryFn<Row, Row, Row>
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

    Map<String, Object> event = ((MapBasedRow) arg1).getEvent();
    if (!MapBasedRow.supportInplaceUpdate(event)) {
      event = Maps.newLinkedHashMap(event);
    }
    // Add aggregations
    for (AggregatorFactory aggregatorFactory : query.getAggregatorSpecs()) {
      final String name = aggregatorFactory.getName();
      event.put(
          name,
          aggregatorFactory.combine(
              arg1.getRaw(name),
              arg2.getRaw(name)
          )
      );
    }
    return new MapBasedRow(adjustTimestamp(arg1), event);
  }

  private DateTime adjustTimestamp(final Row row)
  {
    if (query.getGranularity() instanceof AllGranularity) {
      return row.getTimestamp();
    } else {
      return query.getGranularity().toDateTime(query.getGranularity().truncate(row.getTimestamp().getMillis()));
    }
  }
}
