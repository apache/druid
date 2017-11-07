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

package io.druid.query.aggregation;

import io.druid.guice.annotations.ExtensionPoint;
import io.druid.java.util.common.Cacheable;
import io.druid.java.util.common.UOE;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.ColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * AggregatorFactory is a strategy (in the terms of Design Patterns) that represents column aggregation, e. g. min,
 * max, sum of metric columns, or cardinality of dimension columns (see {@link
 * io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory}).
 */
@ExtensionPoint
public abstract class AggregatorFactory implements Cacheable
{
  private static final Logger log = new Logger(AggregatorFactory.class);

  public abstract Aggregator factorize(ColumnSelectorFactory metricFactory);

  public abstract BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory);

  public abstract Comparator getComparator();

  /**
   * A method that knows how to combine the outputs of {@link Aggregator#get} produced via {@link #factorize} or {@link
   * BufferAggregator#get} produced via {@link #factorizeBuffered}. Note, even though this method is called "combine",
   * this method's contract *does* allow for mutation of the input objects. Thus, any use of lhs or rhs after calling
   * this method is highly discouraged.
   *
   * @param lhs The left hand side of the combine
   * @param rhs The right hand side of the combine
   *
   * @return an object representing the combination of lhs and rhs, this can be a new object or a mutation of the inputs
   */
  public abstract Object combine(Object lhs, Object rhs);

  /**
   * Creates an AggregateCombiner to fold rollup aggregation results from serveral "rows" of different indexes during
   * index merging. AggregateCombiner implements the same logic as {@link #combine}, with the difference that it uses
   * {@link io.druid.segment.ColumnValueSelector} and it's subinterfaces to get inputs and implements {@code
   * ColumnValueSelector} to provide output.
   *
   * @see AggregateCombiner
   * @see io.druid.segment.IndexMerger
   */
  public AggregateCombiner makeAggregateCombiner()
  {
    throw new UOE("[%s] does not implement makeAggregateCombiner()", this.getClass().getName());
  }

  /**
   * Returns an AggregatorFactory that can be used to combine the output of aggregators from this factory.  This
   * generally amounts to simply creating a new factory that is the same as the current except with its input
   * column renamed to the same as the output column.
   *
   * @return a new Factory that can be used for operations on top of data output from the current factory.
   */
  public abstract AggregatorFactory getCombiningFactory();

  /**
   * Returns an AggregatorFactory that can be used to merge the output of aggregators from this factory and
   * other factory.
   * This method is relevant only for AggregatorFactory which can be used at ingestion time.
   *
   * @return a new Factory that can be used for merging the output of aggregators from this factory and other.
   */
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    final AggregatorFactory combiningFactory = this.getCombiningFactory();
    if (other.getName().equals(this.getName()) && combiningFactory.equals(other.getCombiningFactory())) {
      return combiningFactory;
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  /**
   * Gets a list of all columns that this AggregatorFactory will scan
   *
   * @return AggregatorFactories for the columns to scan of the parent AggregatorFactory
   */
  public abstract List<AggregatorFactory> getRequiredColumns();

  /**
   * A method that knows how to "deserialize" the object from whatever form it might have been put into
   * in order to transfer via JSON.
   *
   * @param object the object to deserialize
   *
   * @return the deserialized object
   */
  public abstract Object deserialize(Object object);

  /**
   * "Finalizes" the computation of an object.  Primarily useful for complex types that have a different mergeable
   * intermediate format than their final resultant output.
   *
   * @param object the object to be finalized
   *
   * @return the finalized value that should be returned for the initial query
   */
  public abstract Object finalizeComputation(Object object);

  public abstract String getName();

  public abstract List<String> requiredFields();

  public abstract String getTypeName();

  /**
   * Returns the maximum size that this aggregator will require in bytes for intermediate storage of results.
   *
   * @return the maximum number of bytes that an aggregator of this type will require for intermediate result storage.
   */
  public abstract int getMaxIntermediateSize();

  /**
   * Merges the list of AggregatorFactory[] (presumable from metadata of some segments being merged) and
   * returns merged AggregatorFactory[] (for the metadata for merged segment).
   * Null is returned if it is not possible to do the merging for any of the following reason.
   * - one of the element in input list is null i.e. aggregators for one the segments being merged is unknown
   * - AggregatorFactory of same name can not be merged if they are not compatible
   *
   * @param aggregatorsList
   *
   * @return merged AggregatorFactory[] or Null if merging is not possible.
   */
  @Nullable
  public static AggregatorFactory[] mergeAggregators(List<AggregatorFactory[]> aggregatorsList)
  {
    if (aggregatorsList == null || aggregatorsList.isEmpty()) {
      return null;
    }

    if (aggregatorsList.size() == 1) {
      final AggregatorFactory[] aggregatorFactories = aggregatorsList.get(0);
      if (aggregatorFactories != null) {
        final AggregatorFactory[] combiningFactories = new AggregatorFactory[aggregatorFactories.length];
        Arrays.setAll(combiningFactories, i -> aggregatorFactories[i].getCombiningFactory());
        return combiningFactories;
      } else {
        return null;
      }
    }

    Map<String, AggregatorFactory> mergedAggregators = new LinkedHashMap<>();

    for (AggregatorFactory[] aggregators : aggregatorsList) {

      if (aggregators != null) {
        for (AggregatorFactory aggregator : aggregators) {
          String name = aggregator.getName();
          if (mergedAggregators.containsKey(name)) {
            AggregatorFactory other = mergedAggregators.get(name);
            try {
              // the order of aggregator matters when calling getMergingFactory()
              // because it returns a combiningAggregator which can be different from the original aggregator.
              mergedAggregators.put(name, aggregator.getMergingFactory(other));
            }
            catch (AggregatorFactoryNotMergeableException ex) {
              log.warn(ex, "failed to merge aggregator factories");
              mergedAggregators = null;
              break;
            }
          } else {
            mergedAggregators.put(name, aggregator);
          }
        }
      } else {
        mergedAggregators = null;
        break;
      }
    }

    return mergedAggregators == null
           ? null
           : mergedAggregators.values().toArray(new AggregatorFactory[mergedAggregators.size()]);
  }
}
