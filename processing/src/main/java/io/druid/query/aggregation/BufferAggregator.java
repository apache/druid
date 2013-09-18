/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation;

import java.nio.ByteBuffer;

/**
 * A BufferAggregator is an object that can aggregate metrics into a ByteBuffer.  Its aggregation-related methods
 * (namely, aggregate(...) and get(...)) only take the ByteBuffer and position because it is assumed that the Aggregator
 * was given something (one or more MetricSelector(s)) in its constructor that it can use to get at the next bit of data.
 *
 * Thus, an Aggregator can be thought of as a closure over some other thing that is stateful and changes between calls
 * to aggregate(...).
 */
public interface BufferAggregator
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
   * Release any resources used by the aggregator
   */
  void close();
}
