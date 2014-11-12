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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;


/**
 *
 */
public class TopNNumericResultBuilder implements TopNResultBuilder
{

  private final DateTime timestamp;
  private final DimensionSpec dimSpec;
  private final String metricName;
  private final List<AggregatorFactory> aggFactories;
  private final List<PostAggregator> postAggs;
  private final PriorityQueue<DimValHolder> pQueue;
  private final Comparator<DimValHolder> dimValComparator;
  private static final Comparator<String> dimNameComparator = new Comparator<String>()
  {
    @Override
    public int compare(String o1, String o2)
    {
      int retval;
      if (null == o1) {
        if (null == o2) {
          retval = 0;
        } else {
          retval = -1;
        }
      } else if (null == o2) {
        retval = 1;
      } else {
        retval = o1.compareTo(o2);
      }
      return retval;
    }
  };
  private final int threshold;
  private final Comparator metricComparator;

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
    this.threshold = threshold;
    this.metricComparator = comparator;
    this.dimValComparator = new Comparator<DimValHolder>()
    {
      @Override
      public int compare(DimValHolder d1, DimValHolder d2)
      {
        int retVal = metricComparator.compare(d1.getTopNMetricVal(), d2.getTopNMetricVal());

        if (retVal == 0) {
          retVal = dimNameComparator.compare(d1.getDimName(), d2.getDimName());
        }

        return retVal;
      }
    };

    // The logic in addEntry first adds, then removes if needed. So it can at any point have up to threshold + 1 entries.
    pQueue = new PriorityQueue<>(this.threshold + 1, this.dimValComparator);
  }

  @Override
  public TopNNumericResultBuilder addEntry(
      String dimName,
      Object dimValIndex,
      Object[] metricVals
  )
  {
    final Map<String, Object> metricValues = new LinkedHashMap<>(metricVals.length + postAggs.size());

    metricValues.put(dimSpec.getOutputName(), dimName);

    Iterator<AggregatorFactory> aggFactoryIter = aggFactories.iterator();
    for (Object metricVal : metricVals) {
      metricValues.put(aggFactoryIter.next().getName(), metricVal);
    }

    for (PostAggregator postAgg : postAggs) {
      metricValues.put(postAgg.getName(), postAgg.compute(metricValues));
    }

    Object topNMetricVal = metricValues.get(metricName);

    if (shouldAdd(topNMetricVal)) {
      DimValHolder dimValHolder = new DimValHolder.Builder()
          .withTopNMetricVal(topNMetricVal)
          .withDirName(dimName)
          .withDimValIndex(dimValIndex)
          .withMetricValues(metricValues)
          .build();
      pQueue.add(dimValHolder);
    }
    if (this.pQueue.size() > this.threshold) {
      pQueue.poll();
    }

    return this;
  }

  private boolean shouldAdd(Object topNMetricVal)
  {
    final boolean belowThreshold = pQueue.size() < this.threshold;
    final boolean belowMax = belowThreshold
                             || this.metricComparator.compare(pQueue.peek().getTopNMetricVal(), topNMetricVal) < 0;
    return belowMax;
  }

  @Override
  public TopNResultBuilder addEntry(DimensionAndMetricValueExtractor dimensionAndMetricValueExtractor)
  {
    final Object dimValue = dimensionAndMetricValueExtractor.getDimensionValue(metricName);

    if (shouldAdd(dimValue)) {
      final DimValHolder valHolder = new DimValHolder.Builder()
          .withTopNMetricVal(dimValue)
          .withDirName(dimensionAndMetricValueExtractor.getStringDimensionValue(dimSpec.getOutputName()))
          .withMetricValues(dimensionAndMetricValueExtractor.getBaseObject())
          .build();
      pQueue.add(valHolder);
    }
    if (pQueue.size() > this.threshold) {
      pQueue.poll(); // throw away
    }
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
    final DimValHolder[] holderValueArray = pQueue.toArray(new DimValHolder[0]);
    Arrays.sort(
        holderValueArray, new Comparator<DimValHolder>()
        {
          @Override
          public int compare(DimValHolder d1, DimValHolder d2)
          {
            // Values flipped compared to earlier
            int retVal = metricComparator.compare(d2.getTopNMetricVal(), d1.getTopNMetricVal());

            if (retVal == 0) {
              retVal = dimNameComparator.compare(d1.getDimName(), d2.getDimName());
            }

            return retVal;
          }
        }
    );
    List<DimValHolder> holderValues = Arrays.asList(holderValueArray);

    // Pull out top aggregated values
    final List<Map<String, Object>> values = Lists.transform(
        holderValues,
        new Function<DimValHolder, Map<String, Object>>()
        {
          @Override
          public Map<String, Object> apply(DimValHolder valHolder)
          {
            return valHolder.getMetricValues();
          }
        }
    );
    return new Result<TopNResultValue>(
        timestamp,
        new TopNResultValue(values)
    );
  }
}
