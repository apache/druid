/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.topn;

import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class TopNNumericResultBuilder implements TopNResultBuilder
{
  private final DateTime timestamp;
  private final DimensionSpec dimSpec;
  private final String metricName;
  private final List<AggregatorFactory> aggFactories;
  private final List<PostAggregator> postAggs;
  private MinMaxPriorityQueue<DimValHolder> pQueue = null;

  public TopNNumericResultBuilder(
      DateTime timestamp,
      DimensionSpec dimSpec,
      String metricName,
      int threshold,
      final Comparator comparator,
      List<AggregatorFactory> aggFactories,
      List<PostAggregator> postAggs
  )
  {
    this.timestamp = timestamp;
    this.dimSpec = dimSpec;
    this.metricName = metricName;
    this.aggFactories = aggFactories;
    this.postAggs = AggregatorUtil.pruneDependentPostAgg(postAggs, this.metricName);

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

    metricValues.put(dimSpec.getOutputName(), dimName);

    Iterator<AggregatorFactory> aggFactoryIter = aggFactories.iterator();
    for (Object metricVal : metricVals) {
      metricValues.put(aggFactoryIter.next().getName(), metricVal);
    }

    for (PostAggregator postAgg : postAggs) {
      metricValues.put(postAgg.getName(), postAgg.compute(metricValues));
    }

    Object topNMetricVal = metricValues.get(metricName);
    pQueue.add(
        new DimValHolder.Builder().withTopNMetricVal(topNMetricVal)
                                  .withDirName(dimName)
                                  .withDimValIndex(dimValIndex)
                                  .withMetricValues(metricValues)
                                  .build()
    );

    return this;
  }

  @Override
  public TopNResultBuilder addEntry(DimensionAndMetricValueExtractor dimensionAndMetricValueExtractor)
  {
    pQueue.add(
        new DimValHolder.Builder().withTopNMetricVal(dimensionAndMetricValueExtractor.getDimensionValue(metricName))
                                  .withDirName(dimSpec.getOutputName())
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

    return new Result<TopNResultValue>(
        timestamp,
        new TopNResultValue(values)
    );
  }

  private void instantiatePQueue(int threshold, final Comparator comparator)
  {
    this.pQueue = MinMaxPriorityQueue.orderedBy(
        new Comparator<DimValHolder>()
        {
          @Override
          public int compare(DimValHolder d1, DimValHolder d2)
          {
            int retVal = comparator.compare(d2.getTopNMetricVal(), d1.getTopNMetricVal());

            if (retVal == 0) {
              if (d1.getDimName() == null) {
                retVal = -1;
              } else if (d2.getDimName() == null) {
                retVal = 1;
              } else {
                retVal = d1.getDimName().compareTo(d2.getDimName());
              }
            }

            return retVal;
          }
        }
    ).maximumSize(threshold).create();
  }
}
