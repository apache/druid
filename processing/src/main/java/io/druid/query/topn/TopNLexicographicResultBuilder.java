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
public class TopNLexicographicResultBuilder implements TopNResultBuilder
{
  private final DateTime timestamp;
  private final DimensionSpec dimSpec;
  private final String previousStop;
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

    if (dimName.compareTo(previousStop) > 0) {
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
