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

package org.apache.druid.query.aggregation;

import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.java.util.common.Cacheable;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.PerSegmentQueryOptimizationContext;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ColumnTypeFactory;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * AggregatorFactory is a strategy (in the terms of Design Patterns) that represents column aggregation, e.g. min,
 * max, sum of metric columns, or cardinality of dimension columns (see {@link
 * org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory}).
 * Implementations of {@link AggregatorFactory} which need to Support Nullable Aggregations are encouraged
 * to extend {@link NullableNumericAggregatorFactory}.
 *
 * Implementations are also expected to correctly handle single/multi value string type columns as it makes sense
 * for them e.g. doubleSum aggregator tries to parse the string value as double and assumes it to be zero if parsing
 * fails.
 * If it is a multi value column then each individual value should be taken into account for aggregation e.g. if a row
 * had value ["1","1","1"], doubleSum aggregation would take each of them and sum them to 3.
 */
@ExtensionPoint
public abstract class AggregatorFactory implements Cacheable
{
  private static final Logger log = new Logger(AggregatorFactory.class);

  public abstract Aggregator factorize(ColumnSelectorFactory metricFactory);

  public abstract BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory);

  /**
   * Create a VectorAggregator based on the provided column selector factory. Will throw an exception if
   * this aggregation class does not support vectorization: check "canVectorize" first.
   */
  public VectorAggregator factorizeVector(VectorColumnSelectorFactory selectorFactory)
  {
    throw new UOE("Aggregator[%s] cannot vectorize", getClass().getName());
  }

  /**
   * Creates an {@link Aggregator} based on the provided column selector factory.
   * The returned value is a holder object which contains both the aggregator
   * and its initial size in bytes. The callers can then invoke
   * {@link Aggregator#aggregateWithSize()} to perform aggregation and get back
   * the incremental memory required in each aggregate call. Combined with the
   * initial size, this gives the total on-heap memory required by the aggregator.
   * <p>
   * This method must include JVM object overheads in the estimated size and must
   * ensure not to underestimate required memory as that might lead to OOM errors.
   * <p>
   * This flow does not require invoking {@link #guessAggregatorHeapFootprint(long)}
   * which tends to over-estimate the required memory.
   *
   * @return AggregatorAndSize which contains the actual aggregator and its initial size.
   */
  public AggregatorAndSize factorizeWithSize(ColumnSelectorFactory metricFactory)
  {
    return new AggregatorAndSize(factorize(metricFactory), getMaxIntermediateSize());
  }

  /**
   * Returns whether or not this aggregation class supports vectorization. The default implementation returns false.
   */
  public boolean canVectorize(ColumnInspector columnInspector)
  {
    return false;
  }

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
  @Nullable
  public abstract Object combine(@Nullable Object lhs, @Nullable Object rhs);

  /**
   * Creates an AggregateCombiner to fold rollup aggregation results from serveral "rows" of different indexes during
   * index merging. AggregateCombiner implements the same logic as {@link #combine}, with the difference that it uses
   * {@link org.apache.druid.segment.ColumnValueSelector} and it's subinterfaces to get inputs and implements {@code
   * ColumnValueSelector} to provide output.
   *
   * @see AggregateCombiner
   * @see org.apache.druid.segment.IndexMerger
   */
  public AggregateCombiner makeAggregateCombiner()
  {
    throw new UOE("[%s] does not implement makeAggregateCombiner()", this.getClass().getName());
  }

  /**
   * Creates an {@link AggregateCombiner} which supports nullability.
   * Implementations of {@link AggregatorFactory} which need to Support Nullable Aggregations are encouraged
   * to extend {@link NullableNumericAggregatorFactory} instead of overriding this method.
   * Default implementation calls {@link #makeAggregateCombiner()} for backwards compatibility.
   *
   * @see AggregateCombiner
   * @see NullableNumericAggregatorFactory
   */
  public AggregateCombiner makeNullableAggregateCombiner()
  {
    return makeAggregateCombiner();
  }

  /**
   * Returns an AggregatorFactory that can be used to combine the output of aggregators from this factory. It is used
   * when we know we have some values that were produced with this aggregator factory, and want to do some additional
   * combining of them. This happens, for example, when merging query results from two different segments, or two
   * different servers.
   *
   * For simple aggregators, the combining factory may be computed by simply creating a new factory that is the same as
   * the current, except with its input column renamed to the same as the output column. For example, this aggregator:
   *
   * {"type": "longSum", "fieldName": "foo", "name": "bar"}
   *
   * Would become:
   *
   * {"type": "longSum", "fieldName": "bar", "name": "bar"}
   *
   * Sometimes, the type or other parameters of the combining aggregator will be different from the original aggregator.
   * For example, the {@link CountAggregatorFactory} getCombiningFactory method will return a
   * {@link LongSumAggregatorFactory}, because counts are combined by summing.
   *
   * No matter what, `foo.getCombiningFactory()` and `foo.getCombiningFactory().getCombiningFactory()` should return
   * the same result.
   *
   * @return a new Factory that can be used for operations on top of data output from the current factory.
   */
  public abstract AggregatorFactory getCombiningFactory();

  /**
   * Returns an AggregatorFactory that can be used to combine the output of aggregators from this factory and
   * another factory. It is used when we have some values produced by this aggregator factory, and some values produced
   * by the "other" aggregator factory, and we want to do some additional combining of them. This happens, for example,
   * when compacting two segments together that both have a metric column with the same name. (Even though the name of
   * the column is the same, the aggregator factory used to create it may be different from segment to segment.)
   *
   * This method may throw {@link AggregatorFactoryNotMergeableException}, meaning that "this" and "other" are not
   * compatible and values from one cannot sensibly be combined with values from the other.
   *
   * @return a new Factory that can be used for merging the output of aggregators from this factory and other.
   *
   * @see #getCombiningFactory() which is equivalent to {@code foo.getMergingFactory(foo)} (when "this" and "other"
   * are the same instance).
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
   * Used by {@link org.apache.druid.query.groupby.strategy.GroupByStrategyV1} when running nested groupBys, to
   * "transfer" values from this aggreagtor to an incremental index that the outer query will run on. This method
   * only exists due to the design of GroupByStrategyV1, and should probably not be used for anything else. If you are
   * here because you are looking for a way to get the input fields required by this aggregator, and thought
   * "getRequiredColumns" sounded right, please use {@link #requiredFields()} instead.
   *
   * @return AggregatorFactories that can be used to "transfer" values from this aggregator into an incremental index
   *
   * @see #requiredFields() a similarly-named method that is perhaps the one you want instead.
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
  @Nullable
  public abstract Object finalizeComputation(@Nullable Object object);

  /**
   * @return output name of the aggregator column.
   */
  public abstract String getName();

  /**
   * Get a list of fields that aggregators built by this factory will need to read.
   */
  public abstract List<String> requiredFields();

  /**
   * Get the "intermediate" {@link ColumnType} for this aggregator. This is the same as the type returned by
   * {@link #deserialize} and the type accepted by {@link #combine}. However, it is *not* necessarily the same type
   * returned by {@link #finalizeComputation}.
   *
   * Refer to the {@link ColumnType} javadocs for details on the implications of choosing a type.
   */
  public ColumnType getIntermediateType()
  {
    final ValueType intermediateType = getType();
    if (intermediateType == ValueType.COMPLEX) {
      return ColumnType.ofComplex(getComplexTypeName());
    }
    return ColumnTypeFactory.ofValueType(intermediateType);
  }

  /**
   * Get the {@link ColumnType} for the final form of this aggregator, i.e. the type of the value returned by
   * {@link #finalizeComputation}. This may be the same as or different than the types expected in {@link #deserialize}
   * and {@link #combine}.
   *
   * Refer to the {@link ColumnType} javadocs for details on the implications of choosing a type.
   */
  public ColumnType getResultType()
  {
    // this default 'fill' method is incomplete and can at best return 'unknown' complex
    final ValueType finalized = getFinalizedType();
    if (finalized == ValueType.COMPLEX) {
      return ColumnType.UNKNOWN_COMPLEX;
    }
    return ColumnTypeFactory.ofValueType(finalized);
  }

  /**
   * This method is deprecated and will be removed soon. Use {@link #getIntermediateType()} instead. Do not call this
   * method, it will likely produce incorrect results, it exists for backwards compatibility.
   */
  @Deprecated
  public ValueType getType()
  {
    throw new UnsupportedOperationException(
        "Do not call or implement this method, it is deprecated, use 'getIntermediateType'"
    );
  }

  /**
   * This method is deprecated and will be removed soon. Use {@link #getResultType()} instead. Do not call this
   * method, it will likely produce incorrect results, it exists for backwards compatibility.
   */
  @Deprecated
  public ValueType getFinalizedType()
  {
    throw new UnsupportedOperationException(
        "Do not call or implement this method, it is deprecated, use 'getResultType'"
    );
  }

  /**
   * This method is deprecated and will be removed soon. Use {@link #getIntermediateType()} instead. Do not call this
   * method, it will likely produce incorrect results, it exists for backwards compatibility.
   */
  @Nullable
  @Deprecated
  public String getComplexTypeName()
  {
    return null;
  }

  /**
   * Returns the maximum size that this aggregator will require in bytes for intermediate storage of results.
   *
   * @return the maximum number of bytes that an aggregator of this type will require for intermediate result storage.
   */
  public abstract int getMaxIntermediateSize();

  /**
   * Returns the maximum size that this aggregator will require in bytes for intermediate storage of results.
   * Implementations of {@link AggregatorFactory} which need to Support Nullable Aggregations are encouraged
   * to extend {@link NullableNumericAggregatorFactory} instead of overriding this method.
   * Default implementation calls {@link #makeAggregateCombiner()} for backwards compatibility.
   *
   * @return the maximum number of bytes that an aggregator of this type will require for intermediate result storage.
   */
  public int getMaxIntermediateSizeWithNulls()
  {
    return getMaxIntermediateSize();
  }

  /**
   * Returns a best guess as to how much memory the on-heap {@link Aggregator} returned by {@link #factorize} will
   * require when a certain number of rows have been aggregated into it.
   *
   * The main user of this method is {@link org.apache.druid.segment.incremental.OnheapIncrementalIndex}, which
   * uses it to determine when to persist the current in-memory data to disk.
   *
   * Important note for callers! In nearly all cases, callers that wish to constrain memory would be better off
   * using {@link #factorizeBuffered} or {@link #factorizeVector}, which offer precise control over how much memory
   * is being used.
   */
  public int guessAggregatorHeapFootprint(long rows)
  {
    // By default, guess that on-heap footprint is equal to off-heap footprint.
    return getMaxIntermediateSizeWithNulls();
  }

  /**
   * Return a potentially optimized form of this AggregatorFactory for per-segment queries.
   */
  public AggregatorFactory optimizeForSegment(PerSegmentQueryOptimizationContext optimizationContext)
  {
    return this;
  }

  /**
   * Used in cases where we want to change the output name of the aggregator to something else. For eg: if we have
   * a query `select a, sum(b) as total group by a from table` the aggregator returned from the native group by query is "a0" set in
   * {@link org.apache.druid.sql.calcite.rel.DruidQuery#computeAggregations}. We can use withName("total") to set the output name
   * of the aggregator to "total".
   * <p>
   * As all implementations of this interface method may not exist, callers of this method are advised to handle such a case.
   *
   * @param newName newName of the output for aggregator factory
   * @return AggregatorFactory with the output name set as the input param.
   */
  @SuppressWarnings("unused")
  public AggregatorFactory withName(String newName)
  {
    throw new UOE("Cannot change output name for AggregatorFactory[%s].", this.getClass().getName());
  }

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

    return mergedAggregators == null ? null : mergedAggregators.values().toArray(new AggregatorFactory[0]);
  }
}
