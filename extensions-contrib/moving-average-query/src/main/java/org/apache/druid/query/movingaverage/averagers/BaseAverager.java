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

package org.apache.druid.query.movingaverage.averagers;

import org.apache.druid.query.aggregation.AggregatorFactory;

import java.lang.reflect.Array;
import java.util.Map;

/**
 * Common base class available for use by averagers. The base class implements methods that
 * capture incoming and skipped rows and store them in an array, to be used later for
 * calculating the actual value.
 *
 * @param <I> The type of intermediate value to be retrieved from the row and stored
 * @param <R> The type of result the averager is expected to produce
 */
public abstract class BaseAverager<I, R extends Object> implements Averager<R>
{

  final int numBuckets;
  final int cycleSize;
  private final String name;
  private final String fieldName;
  final I[] buckets;
  private int index;

  /**
   * {@link BaseAverager#startFrom} is needed because `buckets` field is a fixed array, not a list.
   * It makes computeResults() start from the correct bucket in the array.
   */
  int startFrom = 0;

  /**
   * @param storageType    The class to use for storing intermediate values
   * @param numBuckets     The number of buckets to include in the window being aggregated
   * @param name           The name of the resulting metric
   * @param fieldName      The field to extra from incoming rows and stored in the window cache
   * @param cycleSize      Cycle group size. Used to calculate day-of-week option. Default=1 (single element in group).
   */
  public BaseAverager(Class<I> storageType, int numBuckets, String name, String fieldName, int cycleSize)
  {
    this.numBuckets = numBuckets;
    this.name = name;
    this.fieldName = fieldName;
    this.index = 0;
    @SuppressWarnings("unchecked")
    final I[] array = (I[]) Array.newInstance(storageType, numBuckets);
    this.buckets = array;
    this.cycleSize = cycleSize;
  }


  /* (non-Javadoc)
   * @see Averager#addElement(java.util.Map, java.util.Map)
   */
  @SuppressWarnings("unchecked")
  @Override
  public void addElement(Map<String, Object> e, Map<String, AggregatorFactory> a)
  {
    Object metric = e.get(fieldName);
    I finalMetric;
    if (a.containsKey(fieldName)) {
      AggregatorFactory af = a.get(fieldName);
      finalMetric = metric != null ? (I) af.finalizeComputation(metric) : null;
    } else {
      finalMetric = (I) metric;
    }
    buckets[index++] = finalMetric;
    index %= numBuckets;
  }

  /* (non-Javadoc)
   * @see Averager#skip()
   */
  @Override
  public void skip()
  {
    buckets[index++] = null;
    index %= numBuckets;
  }

  /* (non-Javadoc)
   * @see Averager#getResult()
   */
  @Override
  public R getResult()
  {
    if (!hasData()) {
      return null;
    }
    return computeResult();
  }

  /**
   * Compute the result value to be returned by getResult.
   *
   * <p>This routine will only be called when there is valid data within the window
   * and doesn't need to worry about detecting the case where no data should be returned.
   *
   * <p>
   * The method typically should use {@link #getBuckets()} to retrieve the set of buckets
   * within the window and then compute a value based on those. It should expect nulls within
   * the array, indicating buckets where no row was found for the dimension combination. It is
   * up to the actual implementation to determin how to evaluate those nulls.
   *
   * <p>
   * The type returned is NOT required to be the same type as the intermediary value. For example,
   * the intermediate value could be a Sketch, but the result a Long.
   *
   * @return the computed result
   */
  protected abstract R computeResult();

  /* (non-Javadoc)
   * @see Averager#getName()
   */
  @Override
  public String getName()
  {
    return name;
  }

  /**
   * Returns the fieldname to be extracted from any event rows passed in and stored
   * for use computing the windowed function.
   *
   * @return the fieldName
   */
  public String getFieldName()
  {
    return fieldName;
  }

  /**
   * @return the numBuckets
   */
  public int getNumBuckets()
  {
    return numBuckets;
  }

  /**
   * @return the cycleSize
   */
  public int getCycleSize()
  {
    return cycleSize;
  }

  /**
   * @return the array of buckets
   */
  protected I[] getBuckets()
  {
    return buckets;
  }

  /**
   * Determines wheter any data is present. If all the buckets are empty (not "0"), then
   * no value should be returned from the Averager, as there were not valid rows within the window.
   *
   * @return true if any non-null values available
   */
  protected boolean hasData()
  {
    for (Object b : buckets) {
      if (b != null) {
        return true;
      }
    }
    return false;
  }

}
