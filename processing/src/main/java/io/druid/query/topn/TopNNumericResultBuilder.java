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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
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
  private final List<PostAggregator> postAggs;
  private final PriorityQueue<DimValHolder> pQueue;
  private final Comparator<DimValHolder> dimValComparator;
  private final String[] aggFactoryNames;
  private static final Comparator<Comparable> dimNameComparator = new Comparator<Comparable>()
  {
    @Override
    public int compare(Comparable o1, Comparable o2)
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
    this.aggFactoryNames = TopNQueryQueryToolChest.extractFactoryName(aggFactories);

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

  private static final int LOOP_UNROLL_COUNT = 8;

  @Override
  public TopNNumericResultBuilder addEntry(
      Comparable dimName,
      Object dimValIndex,
      Object[] metricVals
  )
  {
    Preconditions.checkArgument(
        metricVals.length == aggFactoryNames.length,
        "metricVals must be the same length as aggFactories"
    );

    final Map<String, Object> metricValues = Maps.newHashMapWithExpectedSize(metricVals.length + postAggs.size() + 1);

    metricValues.put(dimSpec.getOutputName(), dimName);

    final int extra = metricVals.length % LOOP_UNROLL_COUNT;

    switch (extra) {
      case 7:
        metricValues.put(aggFactoryNames[6], metricVals[6]);
        // fall through
      case 6:
        metricValues.put(aggFactoryNames[5], metricVals[5]);
        // fall through
      case 5:
        metricValues.put(aggFactoryNames[4], metricVals[4]);
        // fall through
      case 4:
        metricValues.put(aggFactoryNames[3], metricVals[3]);
        // fall through
      case 3:
        metricValues.put(aggFactoryNames[2], metricVals[2]);
        // fall through
      case 2:
        metricValues.put(aggFactoryNames[1], metricVals[1]);
        // fall through
      case 1:
        metricValues.put(aggFactoryNames[0], metricVals[0]);
    }
    for (int i = extra; i < metricVals.length; i += LOOP_UNROLL_COUNT) {
      metricValues.put(aggFactoryNames[i + 0], metricVals[i + 0]);
      metricValues.put(aggFactoryNames[i + 1], metricVals[i + 1]);
      metricValues.put(aggFactoryNames[i + 2], metricVals[i + 2]);
      metricValues.put(aggFactoryNames[i + 3], metricVals[i + 3]);
      metricValues.put(aggFactoryNames[i + 4], metricVals[i + 4]);
      metricValues.put(aggFactoryNames[i + 5], metricVals[i + 5]);
      metricValues.put(aggFactoryNames[i + 6], metricVals[i + 6]);
      metricValues.put(aggFactoryNames[i + 7], metricVals[i + 7]);
    }

    // Order matters here, do not unroll
    for (PostAggregator postAgg : postAggs) {
      metricValues.put(postAgg.getName(), postAgg.compute(metricValues));
    }

    Object topNMetricVal = metricValues.get(metricName);

    if (shouldAdd(topNMetricVal)) {
      DimValHolder dimValHolder = new DimValHolder.Builder()
          .withTopNMetricVal(topNMetricVal)
          .withDimName(dimName)
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
          .withDimName((Comparable) dimensionAndMetricValueExtractor.getDimensionValue(dimSpec.getOutputName()))
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
