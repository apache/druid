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
import org.apache.druid.query.monomorphicprocessing.CalledFromHotLoop;
import org.apache.druid.query.monomorphicprocessing.HotLoopCallee;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * A BufferAggregator is an object that can aggregate metrics into a ByteBuffer.  Its aggregation-related methods
 * (namely, aggregate(...) and get(...)) only take the ByteBuffer and position because it is assumed that the Aggregator
 * was given something (one or more MetricSelector(s)) in its constructor that it can use to get at the next bit of data.
 *
 * Thus, an Aggregator can be thought of as a closure over some other thing that is stateful and changes between calls
 * to aggregate(...).
 *
 * @see VectorAggregator, the vectorized version
 */
@ExtensionPoint
public interface BufferAggregator extends BaseBufferAggregator, HotLoopCallee
{
  /**
   * {@inheritDoc}
   *
   * Overridden because this method is a {@link HotLoopCallee} and the superinterface method is not.
   */
  @Override
  @CalledFromHotLoop
  void init(ByteBuffer buf, int position);

  /**
   * Aggregates metric values into the given aggregate byte representation
   *
   * Implementations of this method must read in the aggregate value from the buffer at the given position,
   * aggregate the next element of data and write the updated aggregate value back into the buffer.
   *
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   *
   * @param buf      byte buffer storing the byte array representation of the aggregate
   * @param position offset within the byte buffer at which the current aggregate value is stored
   */
  @CalledFromHotLoop
  void aggregate(ByteBuffer buf, int position);

  /**
   * {@inheritDoc}
   *
   * Overridden because this method is a {@link HotLoopCallee} and the superinterface method is not.
   */
  @Override
  @Nullable
  @CalledFromHotLoop
  Object get(ByteBuffer buf, int position);

  /**
   * Returns the float representation of the given aggregate byte array
   *
   * Converts the given byte buffer representation into the intermediate aggregate value.
   *
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   *
   * Implementations are only required to support this method if they are aggregations which
   * have an {@link AggregatorFactory#getType()} ()} of {@link org.apache.druid.segment.column.ValueType#FLOAT}.
   * If unimplemented, throwing an {@link UnsupportedOperationException} is common and recommended.
   *
   * @param buf      byte buffer storing the byte array representation of the aggregate
   * @param position offset within the byte buffer at which the aggregate value is stored
   *
   * @return the float representation of the aggregate
   */
  float getFloat(ByteBuffer buf, int position);

  /**
   * Returns the long representation of the given aggregate byte array
   *
   * Converts the given byte buffer representation into the intermediate aggregate value.
   *
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   *
   * Implementations are only required to support this method if they are aggregations which
   * have an {@link AggregatorFactory#getType()} of  of {@link org.apache.druid.segment.column.ValueType#LONG}.
   * If unimplemented, throwing an {@link UnsupportedOperationException} is common and recommended.
   *
   * @param buf      byte buffer storing the byte array representation of the aggregate
   * @param position offset within the byte buffer at which the aggregate value is stored
   *
   * @return the long representation of the aggregate
   */
  long getLong(ByteBuffer buf, int position);

  /**
   * Returns the double representation of the given aggregate byte array
   *
   * Converts the given byte buffer representation into the intermediate aggregate value.
   *
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   *
   * Implementations are only required to support this method if they are aggregations which
   * have an {@link AggregatorFactory#getType()} of  of {@link org.apache.druid.segment.column.ValueType#DOUBLE}.
   * If unimplemented, throwing an {@link UnsupportedOperationException} is common and recommended.
   *
   * The default implementation casts {@link BufferAggregator#getFloat(ByteBuffer, int)} to double.
   * This default method is added to enable smooth backward compatibility, please re-implement it if your aggregators
   * work with numeric double columns.
   *
   * @param buf      byte buffer storing the byte array representation of the aggregate
   * @param position offset within the byte buffer at which the aggregate value is stored
   *
   * @return the double representation of the aggregate
   */
  default double getDouble(ByteBuffer buf, int position)
  {
    return (double) getFloat(buf, position);
  }

  /**
   * {@inheritDoc}
   *
   * <p>The default implementation inspects nothing. Classes that implement {@code BufferAggregator} are encouraged to
   * override this method, following the specification of {@link HotLoopCallee#inspectRuntimeShape}.
   */
  @Override
  default void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // nothing to inspect
  }

  /**
   * returns true if aggregator's output type is primitive long/double/float and aggregated value is null,
   * but when aggregated output type is Object, this method always returns false,
   * and users are advised to check nullability for the object returned by {@link BufferAggregator#get(ByteBuffer, int)}
   * method.
   * The default implementation always return false to enable smooth backward compatibility,
   * re-implement if your aggregator is nullable.
   *
   * @param buf      byte buffer storing the byte array representation of the aggregate
   * @param position offset within the byte buffer at which the aggregate value is stored
   *
   * @return true if the aggregated value is primitive long/double/float and aggregated value is null otherwise false.
   */
  default boolean isNull(ByteBuffer buf, int position)
  {
    return false;
  }
}
