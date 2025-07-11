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

package org.apache.druid.query.aggregation.exact.count.bitmap64;

import java.nio.ByteBuffer;

/**
 * Interface for a 64-bit bitmap counter used for exact cardinality estimation.
 * <p>
 * Implementations of this interface provide methods to add values, merge counters, retrieve the cardinality,
 * and serialize the counter state. This is typically used in scenarios where exact distinct counting is required
 * for large datasets, such as analytics or aggregation queries in Druid.
 * </p>
 */
public interface Bitmap64
{
  /**
   * Adds a value to the bitmap counter. The value is treated as a unique element to be tracked for cardinality.
   *
   * @param value the value to add to the counter
   */
  void add(long value);

  /**
   * Returns the exact cardinality (number of unique values) currently tracked by this counter.
   *
   * @return the number of unique values added to the counter
   */
  long getCardinality();

  /**
   * Merges the unique values from another Bitmap64Counter into this one, resulting in a new counter that represents the union of both counters.
   * This method modifies and returns the current Bitmap64Counter instance.
   *
   * @param rhs the other Bitmap64Counter to merge with
   * @return a new Bitmap64Counter representing the union of both counters
   */
  Bitmap64 fold(Bitmap64 rhs);

  /**
   * Serializes the current state of the counter into a ByteBuffer.
   *
   * @return a ByteBuffer containing the serialized state of the counter
   */
  ByteBuffer toByteBuffer();
}
