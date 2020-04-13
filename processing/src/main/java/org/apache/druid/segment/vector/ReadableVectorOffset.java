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

package org.apache.druid.segment.vector;

/**
 * Provides a batch of offsets, ostensibly as indexes into an array.
 *
 * A ReadableVectorOffset should be given to classes (e.g. column selector objects) by something which keeps a
 * reference to the base VectorOffset object and increments it.
 *
 * @see VectorOffset, the movable version.
 * @see org.apache.druid.segment.data.ReadableOffset, the non-vectorized version.
 */
public interface ReadableVectorOffset extends VectorSizeInspector
{
  /**
   * A marker value that will never be returned by "getId".
   */
  int NULL_ID = -1;

  /**
   * Returns an integer that uniquely identifies the current position of the offset. Should *not* be construed as an
   * actual offset; for that, use "getStartOffset" or "getOffsets". This is useful for caching: it is safe to assume
   * nothing has changed in the offset so long as the id remains the same.
   */
  int getId();

  /**
   * Checks if the current batch is a contiguous range or not. This is only good for one batch at a time, since the
   * same object may return some contiguous batches and some non-contiguous batches. So, callers must check this method
   * each time they want to retrieve the current batch of offsets.
   */
  boolean isContiguous();

  /**
   * If "isContiguous" is true, this method returns the start offset of the range. The length of the range is
   * given by "getCurrentVectorSize".
   *
   * Throws an exception if "isContiguous" is false.
   */
  int getStartOffset();

  /**
   * If "isContiguous" is false, this method returns a batch of offsets. The array may be longer than the number of
   * valid offsets, so callers need to check "getCurrentVectorSize" too.
   *
   * Throws an exception if "isContiguous" is true.
   */
  int[] getOffsets();
}
