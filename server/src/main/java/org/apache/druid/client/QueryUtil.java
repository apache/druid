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

package org.apache.druid.client;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.timeline.DataSegment;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class QueryUtil
{

  public static <T> Set<T> filterShards(
      Set<String> requiredFields,
      Iterable<T> input,
      Function<T, DataSegment> converter
  )
  {
    Set<T> retSet = new LinkedHashSet<>();
    for (T obj : input) {
      DataSegment segment = converter.apply(obj);
      if ((segment.getDimensions() != null && segment.getDimensions()
          .stream()
          .anyMatch(c -> requiredFields.contains(c)))
          || (segment.getMetrics() != null && segment.getMetrics()
          .stream()
          .anyMatch(m -> requiredFields.contains(m)))) {
        retSet.add(obj);
      }
    }
    return retSet;
  }

  public static Optional<Set<String>> getRequiredFields(Query query)
  {
    // Only support Scan, Timeseries, TopN, and GroupBy
    Set<String> dimensions = new HashSet<>();
    if (query instanceof TopNQuery) {
      TopNQuery q = (TopNQuery) query;
      dimensions.addAll(extractFieldsFromAggregations(q.getAggregatorSpecs()));
    } else if (query instanceof TimeseriesQuery) {
      TimeseriesQuery q = (TimeseriesQuery) query;
      dimensions.addAll(extractFieldsFromAggregations(q.getAggregatorSpecs()));
    } else if (query instanceof GroupByQuery) {
      GroupByQuery q = (GroupByQuery) query;
      dimensions.addAll(extractFieldsFromAggregations(q.getAggregatorSpecs()));
    } else if (query instanceof ScanQuery) {
      ScanQuery q = (ScanQuery) query;
      dimensions.addAll(q.getColumns());
    } else {
      return Optional.absent();
    }
    return Optional.of(dimensions);
  }

  private static Set<String> extractFieldsFromAggregations(List<AggregatorFactory> aggs)
  {
    Set<String> ret = new HashSet<>();
    for (AggregatorFactory agg : aggs) {
      if (agg instanceof FilteredAggregatorFactory) {
        FilteredAggregatorFactory fagg = (FilteredAggregatorFactory) agg;
        ret.addAll(fagg.getFilter().getRequiredColumns());
      }
      ret.addAll(agg.requiredFields());
    }
    return ret;
  }
}
