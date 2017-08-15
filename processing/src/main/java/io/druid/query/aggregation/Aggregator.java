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

import java.io.Closeable;

/**
 * An Aggregator is an object that can aggregate metrics.  Its aggregation-related methods (namely, aggregate() and get())
 * do not take any arguments as the assumption is that the Aggregator was given something in its constructor that
 * it can use to get at the next bit of data.
 *
 * Thus, an Aggregator can be thought of as a closure over some other thing that is stateful and changes between calls
 * to aggregate().  This is currently (as of this documentation) implemented through the use of Offset and
 * FloatColumnSelector objects.  The Aggregator has a handle on a FloatColumnSelector object which has a handle on an Offset.
 * QueryableIndex has both the Aggregators and the Offset object and iterates through the Offset calling the aggregate()
 * method on the Aggregators for each applicable row.
 *
 * This interface is old and going away.  It is being replaced by BufferAggregator
 */
public interface Aggregator extends Closeable
{
  void aggregate();
  void reset();
  Object get();
  float getFloat();
  long getLong();

  /**
   * The default implementation casts {@link Aggregator#getFloat()} to double.
   * This default method is added to enable smooth backward compatibility, please re-implement it if your aggregators
   * work with numeric double columns.
   */
  default double getDouble()
  {
    return (double) getFloat();
  }

  @Override
  void close();
}
