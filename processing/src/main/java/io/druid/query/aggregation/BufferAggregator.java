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

import java.nio.ByteBuffer;

/**
 * A BufferAggregator is an object that can aggregate metrics into a ByteBuffer.  Its aggregation-related methods
 * (namely, aggregate(...) and get(...)) only take the ByteBuffer and position because it is assumed that the Aggregator
 * was given something (one or more MetricSelector(s)) in its constructor that it can use to get at the next bit of data.
 * <p>
 * Thus, an Aggregator can be thought of as a closure over some other thing that is stateful and changes between calls
 * to aggregate(...).
 */
public interface BufferAggregator
{
  /**
   * Initializes the buffer location
   * <p>
   * Implementations of this method must initialize the byte buffer at the given position
   * <p>
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   * <p>
   * This method must not exceed the number of bytes returned by {@link AggregatorFactory#getMaxIntermediateSize()}
   * in the corresponding {@link AggregatorFactory}
   *
   * @param buf      byte buffer to initialize
   * @param position offset within the byte buffer for initialization
   */
  void init(ByteBuffer buf, int position);

  /**
   * Aggregates metric values into the given aggregate byte representation
   * <p>
   * Implementations of this method must read in the aggregate value from the buffer at the given position,
   * aggregate the next element of data and write the updated aggregate value back into the buffer.
   * <p>
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   *
   * @param buf      byte buffer storing the byte array representation of the aggregate
   * @param position offset within the byte buffer at which the current aggregate value is stored
   */
  void aggregate(ByteBuffer buf, int position);

  /**
   * Returns the intermediate object representation of the given aggregate.
   * <p>
   * Converts the given byte buffer representation into an intermediate aggregate Object
   * <p>
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   *
   * @param buf      byte buffer storing the byte array representation of the aggregate
   * @param position offset within the byte buffer at which the aggregate value is stored
   *
   * @return the Object representation of the aggregate
   */
  Object get(ByteBuffer buf, int position);

  /**
   * Returns the float representation of the given aggregate byte array
   * <p>
   * Converts the given byte buffer representation into the intermediate aggregate value.
   * <p>
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   * <p>
   * Implementations are only required to support this method if they are aggregations which
   * have an {@link AggregatorFactory#getTypeName()} of "float".
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
   * <p>
   * Converts the given byte buffer representation into the intermediate aggregate value.
   * <p>
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   * <p>
   * Implementations are only required to support this method if they are aggregations which
   * have an {@link AggregatorFactory#getTypeName()} of "long".
   * If unimplemented, throwing an {@link UnsupportedOperationException} is common and recommended.
   *
   * @param buf      byte buffer storing the byte array representation of the aggregate
   * @param position offset within the byte buffer at which the aggregate value is stored
   *
   * @return the long representation of the aggregate
   */
  long getLong(ByteBuffer buf, int position);

  /**
   * Returns the int representation of the given aggregate byte array
   * <p>
   * Converts the given byte buffer representation into the intermediate aggregate value.
   * <p>
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   * <p>
   * Implementations are only required to support this method if they are aggregations which
   * have an {@link AggregatorFactory#getTypeName()} of "long".
   * If unimplemented, throwing an {@link UnsupportedOperationException} is common and recommended.
   *
   * @param buf      byte buffer storing the byte array representation of the aggregate
   * @param position offset within the byte buffer at which the aggregate value is stored
   *
   * @return the int representation of the aggregate
   */
  int getInt(ByteBuffer buf, int position);

  /**
   * Returns the double representation of the given aggregate byte array
   * <p>
   * Converts the given byte buffer representation into the intermediate aggregate value.
   * <p>
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   * <p>
   * Implementations are only required to support this method if they are aggregations which
   * have an {@link AggregatorFactory#getTypeName()} of "long".
   * If unimplemented, throwing an {@link UnsupportedOperationException} is common and recommended.
   *
   * @param buf      byte buffer storing the byte array representation of the aggregate
   * @param position offset within the byte buffer at which the aggregate value is stored
   *
   * @return the double representation of the aggregate
   */
  double getDouble(ByteBuffer buf, int position);

  /**
   * Release any resources used by the aggregator
   */
  void close();
}
