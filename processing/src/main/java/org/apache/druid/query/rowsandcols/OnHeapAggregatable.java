package org.apache.druid.query.rowsandcols;

import org.apache.druid.query.aggregation.AggregatorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A semantic interface used to aggregate a list of AggregatorFactories across a given set of data
 * <p>
 * The aggregation specifically happens on-heap and should be used in places where it is known that the data
 * set can be worked with entirely on-heap.
 * <p>
 * Note, as we implement frame-handling for window aggregations, it is expected that this interface will undergo a
 * transformation.  It might be deleted and replaced with something else, or might just see a change done in place.
 * Either way, there is no assumption of enforced compatibility with this interface at this point in time.
 */
public interface OnHeapAggregatable
{
  /**
   * Aggregates the data using the {@code List<AggregatorFactory} objects.
   *
   * @param aggFactories definition of aggregations to be done
   * @return a list of objects, one per AggregatorFactory.  That is, the length of the return list should be equal to
   * the length of the aggFactories list passed as an argument
   */
  ArrayList<Object> aggregateAll(List<AggregatorFactory> aggFactories);
}
