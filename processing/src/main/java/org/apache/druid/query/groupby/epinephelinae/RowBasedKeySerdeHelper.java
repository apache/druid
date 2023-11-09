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

package org.apache.druid.query.groupby.epinephelinae;

import org.apache.druid.query.groupby.epinephelinae.Grouper.BufferComparator;
import org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKey;

import javax.annotation.CheckReturnValue;
import java.nio.ByteBuffer;

interface RowBasedKeySerdeHelper
{
  /**
   * @return The size in bytes for a value of the column handled by this SerdeHelper.
   */
  int getKeyBufferValueSize();

  /**
   * Read a value from RowBasedKey at `idx` and put the value at the current position of RowBasedKeySerde's keyBuffer.
   * advancing the position by the size returned by getKeyBufferValueSize().
   *
   * If an internal resource limit has been reached and the value could not be added to the keyBuffer,
   * (e.g., maximum dictionary size exceeded for Strings), this method returns false.
   *
   * @param key RowBasedKey containing the grouping key values for a row.
   * @param idx Index of the grouping key column within that this SerdeHelper handles
   *
   * @return true if the value was added to the key, false otherwise
   */
  @CheckReturnValue
  boolean putToKeyBuffer(RowBasedKey key, int idx);

  /**
   * Read a value from a ByteBuffer containing a grouping key in the same format as RowBasedKeySerde's keyBuffer and
   * put the value in `dimValues` at `dimValIdx`.
   *
   * The value to be read resides in the buffer at position (`initialOffset` + the SerdeHelper's keyBufferPosition).
   *
   * @param buffer        ByteBuffer containing an array of grouping keys for a row
   * @param initialOffset Offset where non-timestamp grouping key columns start, needed because timestamp is not
   *                      always included in the buffer.
   * @param dimValIdx     Index within dimValues to store the value read from the buffer
   * @param dimValues     Output array containing grouping key values for a row
   */
  void getFromByteBuffer(ByteBuffer buffer, int initialOffset, int dimValIdx, Object[] dimValues);

  /**
   * Return a {@link BufferComparator} to compare keys stored in ByteBuffer.
   */
  BufferComparator getBufferComparator();
}
