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

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Contains methods common to {@link BufferAggregator} and {@link VectorAggregator}.
 */
public interface BaseBufferAggregator
{
  /**
   * Initializes the buffer location
   *
   * Implementations of this method must initialize the byte buffer at the given position
   *
   * <b>Implementations must not change the position, limit or mark of the given buffer</b>
   *
   * This method must not exceed the number of bytes returned by {@link AggregatorFactory#getMaxIntermediateSizeWithNulls}
   * in the corresponding {@link AggregatorFactory}
   *
   * @param buf      byte buffer to initialize
   * @param position offset within the byte buffer for initialization
   */
  void init(ByteBuffer buf, int position);

  /**
   * Returns the intermediate object representation of the given aggregate.
   *
   * Converts the given byte buffer representation into an intermediate aggregate Object
   *
   * <b>Implementations must not change the position, limit or mark of the given buffer.</b>
   *
   * <b>
   * The object returned must not have any references to the given buffer (i.e., make a copy), since the
   * underlying buffer is a shared resource and may be given to another processing thread
   * while the objects returned by this aggregator are still in use.
   * </b>
   *
   * <b>If the corresponding {@link AggregatorFactory#combine(Object, Object)} method for this aggregator
   * expects its inputs to be mutable, then the object returned by this method must be mutable.</b>
   *
   * @param buf      byte buffer storing the byte array representation of the aggregate
   * @param position offset within the byte buffer at which the aggregate value is stored
   *
   * @return the Object representation of the aggregate
   */
  @Nullable
  Object get(ByteBuffer buf, int position);

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
   * @param oldBuffer   old aggregation buffer.
   * @param newBuffer   new aggregation buffer.
   */
  default void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
  }

  /**
   * Release any resources used by the aggregator
   */
  void close();
}
