package com.metamx.druid.aggregation;

import java.util.Comparator;

/**
 * An Aggregator is an object that can aggregate metrics.  Its aggregation-related methods (namely, aggregate() and get())
 * do not take any arguments as the assumption is that the Aggregator was given something in its constructor that
 * it can use to get at the next bit of data.
 *
 * Thus, an Aggregator can be thought of as a closure over some other thing that is stateful and changes between calls
 * to aggregate().  This is currently (as of this documentation) implemented through the use of Offset and
 * FloatMetricSelector objects.  The Aggregator has a handle on a FloatMetricSelector object which has a handle on an Offset.
 * QueryableIndex has both the Aggregators and the Offset object and iterates through the Offset calling the aggregate()
 * method on the Aggregators for each applicable row.
 *
 * This interface is old and going away.  It is being replaced by BufferAggregator
 */
public interface Aggregator {
  void aggregate();
  void reset();
  Object get();
  float getFloat();
  String getName();
}
