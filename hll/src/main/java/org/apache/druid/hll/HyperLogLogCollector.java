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

package org.apache.druid.hll;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public interface HyperLogLogCollector extends Comparable<HyperLogLogCollector>
{

  long estimateCardinalityRound();

  double estimateCardinality();

  void add(byte[] hashedValue);

  HyperLogLogCollector fold(@Nullable HyperLogLogCollector other);

  HyperLogLogCollector fold(ByteBuffer buffer);

  ByteBuffer toByteBuffer();

  byte[] toByteArray();

  int getNumBuckets();

  // Methods to build the latest HLLC
  static HyperLogLogCollector makeLatestCollector()
  {
    return AbstractHyperLogLogCollector.makeLatestCollector();
  }

  /**
   * Create a wrapper object around an HLL sketch contained within a buffer. The position and limit of
   * the buffer may be changed; if you do not want this to happen, you can duplicate the buffer before
   * passing it in.
   * <p>
   * The mark and byte order of the buffer will not be modified.
   *
   * @param buffer buffer containing an HLL sketch starting at its position and ending at its limit
   * @return HLLC wrapper object
   */
  static HyperLogLogCollector makeCollector(ByteBuffer buffer)
  {
    return AbstractHyperLogLogCollector.makeCollector(buffer);
  }

  static double estimateByteBuffer(ByteBuffer buf)
  {
    return makeCollector(buf.duplicate()).estimateCardinality();
  }

  /**
   * Creates new collector which shares others collector buffer (by using {@link ByteBuffer#duplicate()})
   *
   * @param otherCollector collector which buffer will be shared
   * @return collector
   */
  static HyperLogLogCollector makeCollectorSharingStorage(HyperLogLogCollector otherCollector)
  {
    AbstractHyperLogLogCollector collector = (AbstractHyperLogLogCollector) otherCollector;
    return makeCollector(collector.getStorageBuffer().duplicate());
  }

  static int getLatestNumBytesForDenseStorage()
  {
    return VersionOneHyperLogLogCollector.NUM_BYTES_FOR_DENSE_STORAGE;
  }

  static byte[] makeEmptyVersionedByteArray()
  {
    byte[] arr = new byte[getLatestNumBytesForDenseStorage()];
    arr[0] = VersionOneHyperLogLogCollector.VERSION;
    return arr;
  }
}
