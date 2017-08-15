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

import io.druid.query.monomorphicprocessing.CalledFromHotLoop;
import io.druid.query.monomorphicprocessing.HotLoopCallee;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import java.nio.ByteBuffer;

/**
 * A BufferAggregator is an object that can aggregate metrics into a ByteBuffer.  Its aggregation-related methods
 * (namely, aggregate(...) and get(...)) only take the ByteBuffer and position because it is assumed that the Aggregator
 * was given something (one or more MetricSelector(s)) in its constructor that it can use to get at the next bit of data.
 *
 * Thus, an Aggregator can be thought of as a closure over some other thing that is stateful and changes between calls
 * to aggregate(...).
 */
public interface BufferAggregator extends HotLoopCallee
{
  /**
   * Initializes the buffer location
   *
   * Implementations of this method must initialize the byte buffer at the given position
   *
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   *
   * This method must not exceed the number of bytes returned by {@link AggregatorFactory#getMaxIntermediateSize()}
   * in the corresponding {@link AggregatorFactory}
   *
   * @param buf byte buffer to initialize
   * @param position offset within the byte buffer for initialization
   */
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
   * @param buf byte buffer storing the byte array representation of the aggregate
   * @param position offset within the byte buffer at which the current aggregate value is stored
   */
  @CalledFromHotLoop
  void aggregate(ByteBuffer buf, int position);

  /**
   * Returns the intermediate object representation of the given aggregate.
   *
   * Converts the given byte buffer representation into an intermediate aggregate Object
   *
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   *
   * @param buf byte buffer storing the byte array representation of the aggregate
   * @param position offset within the byte buffer at which the aggregate value is stored
   * @return the Object representation of the aggregate
   */
  Object get(ByteBuffer buf, int position);

  /**
   * Returns the float representation of the given aggregate byte array
   *
   * Converts the given byte buffer representation into the intermediate aggregate value.
   *
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   *
   * Implementations are only required to support this method if they are aggregations which
   * have an {@link AggregatorFactory#getTypeName()} of "float".
   * If unimplemented, throwing an {@link UnsupportedOperationException} is common and recommended.
   *
   * @param buf byte buffer storing the byte array representation of the aggregate
   * @param position offset within the byte buffer at which the aggregate value is stored
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
   * have an {@link AggregatorFactory#getTypeName()} of "long".
   * If unimplemented, throwing an {@link UnsupportedOperationException} is common and recommended.
   *
   * @param buf byte buffer storing the byte array representation of the aggregate
   * @param position offset within the byte buffer at which the aggregate value is stored
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
   * have an {@link AggregatorFactory#getTypeName()} of "double".
   * If unimplemented, throwing an {@link UnsupportedOperationException} is common and recommended.
   *
   * The default implementation casts {@link BufferAggregator#getFloat(ByteBuffer, int)} to double.
   * This default method is added to enable smooth backward compatibility, please re-implement it if your aggregators
   * work with numeric double columns.
   *
   * @param buf byte buffer storing the byte array representation of the aggregate
   * @param position offset within the byte buffer at which the aggregate value is stored
   * @return the double representation of the aggregate
   */
  default double getDouble(ByteBuffer buf, int position)
  {
    return (double) getFloat(buf, position);
  }

  /**
   * Release any resources used by the aggregator
   */
  void close();

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
   * Relocates any cached objects.
   * If underlying ByteBuffer used for aggregation buffer relocates to a new ByteBuffer, positional caches(if any)
   * built on top of old ByteBuffer can not be used for further {@link BufferAggregator#aggregate(ByteBuffer, int)}
   * calls. This method tells the BufferAggregator that the cached objects at a certain location has been relocated to
   * a different location.
   *
   * Only used if there is any positional caches/objects in the BufferAggregator implementation.
   *
   * If relocate happens to be across multiple new ByteBuffers (say n ByteBuffers), this method should be called
   * multiple times(n times) given all the new positions/old positions should exist in newBuffer/OldBuffer.
   *
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   *
   * @param oldPosition old position of a cached object before aggregation buffer relocates to a new ByteBuffer.
   * @param newPosition new position of a cached object after aggregation buffer relocates to a new ByteBuffer.
   * @param oldBuffer old aggregation buffer.
   * @param newBuffer new aggregation buffer.
   */
  default void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
  }

}
