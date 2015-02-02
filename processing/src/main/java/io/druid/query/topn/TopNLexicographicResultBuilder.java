/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.topn;

import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class TopNLexicographicResultBuilder implements TopNResultBuilder
{
  private final DateTime timestamp;
  private final DimensionSpec dimSpec;
  private final String previousStop;
  private final Comparator comparator;
  private final List<AggregatorFactory> aggFactories;
  private MinMaxPriorityQueue<DimValHolder> pQueue = null;

  public TopNLexicographicResultBuilder(
      DateTime timestamp,
      DimensionSpec dimSpec,
      int threshold,
      String previousStop,
      final Comparator comparator,
      List<AggregatorFactory> aggFactories
  )
  {
    this.timestamp = timestamp;
    this.dimSpec = dimSpec;
    this.previousStop = previousStop;
    this.comparator = comparator;
    this.aggFactories = aggFactories;

    instantiatePQueue(threshold, comparator);
  }

  @Override
  public TopNResultBuilder addEntry(
      String dimName,
      Object dimValIndex,
      Object[] metricVals
  )
  {
    Map<String, Object> metricValues = Maps.newLinkedHashMap();

    if (comparator.compare(dimName, previousStop) > 0) {
      metricValues.put(dimSpec.getOutputName(), dimName);
      Iterator<AggregatorFactory> aggsIter = aggFactories.iterator();
      for (Object metricVal : metricVals) {
        metricValues.put(aggsIter.next().getName(), metricVal);
      }

      pQueue.add(new DimValHolder.Builder().withDirName(dimName).withMetricValues(metricValues).build());
    }

    return this;
  }

  @Override
  public TopNResultBuilder addEntry(DimensionAndMetricValueExtractor dimensionAndMetricValueExtractor)
  {
    pQueue.add(
        new DimValHolder.Builder().withDirName(dimensionAndMetricValueExtractor.getStringDimensionValue(dimSpec.getOutputName()))
                                  .withMetricValues(dimensionAndMetricValueExtractor.getBaseObject())
                                  .build()
    );

    return this;
  }

  @Override
  public Iterator<DimValHolder> getTopNIterator()
  {
    return pQueue.iterator();
  }

  @Override
  public Result<TopNResultValue> build()
  {
    // Pull out top aggregated values
    List<Map<String, Object>> values = new ArrayList<Map<String, Object>>(pQueue.size());
    while (!pQueue.isEmpty()) {
      values.add(pQueue.remove().getMetricValues());
    }

    return new Result<TopNResultValue>(timestamp, new TopNResultValue(values));
  }

  private void instantiatePQueue(int threshold, final Comparator comparator)
  {
    this.pQueue = MinMaxPriorityQueue.orderedBy(
        new Comparator<DimValHolder>()
        {
          @Override
          public int compare(
              DimValHolder o1,
              DimValHolder o2
          )
          {
            return comparator.compare(o1.getDimName(), o2.getDimName());
          }
        }
    ).maximumSize(threshold).create();
  }
}
