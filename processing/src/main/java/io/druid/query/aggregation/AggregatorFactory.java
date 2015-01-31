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

package io.druid.query.aggregation;

import io.druid.segment.ColumnSelectorFactory;

import java.util.Comparator;
import java.util.List;

/**
 * Processing related interface
 * 
 * An AggregatorFactory is an object that knows how to generate an Aggregator using a ColumnSelectorFactory.
 * 
 * This is useful as an abstraction to allow Aggregator classes to be written in terms of MetricSelector objects
 * without making any assumptions about how they are pulling values out of the base data.  That is, the data is
 * provided to the Aggregator through the MetricSelector object, so whatever creates that object gets to choose how
 * the data is actually stored and accessed.
 */
public interface AggregatorFactory
{
  public Aggregator factorize(ColumnSelectorFactory metricFactory);

  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory);

  public Comparator getComparator();

  /**
   * A method that knows how to combine the outputs of the getIntermediate() method from the Aggregators
   * produced via factorize().  Note, even though this is called combine, this method's contract *does*
   * allow for mutation of the input objects.  Thus, any use of lhs or rhs after calling this method is
   * highly discouraged.
   *
   * @param lhs The left hand side of the combine
   * @param rhs The right hand side of the combine
   *
   * @return an object representing the combination of lhs and rhs, this can be a new object or a mutation of the inputs
   */
  public Object combine(Object lhs, Object rhs);

  /**
   * Returns an AggregatorFactory that can be used to combine the output of aggregators from this factory.  This
   * generally amounts to simply creating a new factory that is the same as the current except with its input
   * column renamed to the same as the output column.
   *
   * @return a new Factory that can be used for operations on top of data output from the current factory.
   */
  public AggregatorFactory getCombiningFactory();

  /**
   * Gets a list of all columns that this AggregatorFactory will scan
   *
   * @return AggregatorFactories for the columns to scan of the parent AggregatorFactory
   */
  public List<AggregatorFactory> getRequiredColumns();

  /**
   * A method that knows how to "deserialize" the object from whatever form it might have been put into
   * in order to transfer via JSON.
   *
   * @param object the object to deserialize
   *
   * @return the deserialized object
   */
  public Object deserialize(Object object);

  /**
   * "Finalizes" the computation of an object.  Primarily useful for complex types that have a different mergeable
   * intermediate format than their final resultant output.
   *
   * @param object the object to be finalized
   *
   * @return the finalized value that should be returned for the initial query
   */
  public Object finalizeComputation(Object object);

  public String getName();

  public List<String> requiredFields();

  public byte[] getCacheKey();

  public String getTypeName();

  /**
   * Returns the maximum size that this aggregator will require in bytes for intermediate storage of results.
   *
   * @return the maximum number of bytes that an aggregator of this type will require for intermediate result storage.
   */
  public int getMaxIntermediateSize();

  /**
   * Returns the starting value for a corresponding aggregator. For example, 0 for sums, - Infinity for max, an empty mogrifier
   *
   * @return the starting value for a corresponding aggregator.
   */
  public Object getAggregatorStartValue();
}
