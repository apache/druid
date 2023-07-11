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

package org.apache.druid.frame.channel;

import java.io.Closeable;
import java.io.IOException;

/**
 * Provides an interface to read a partitioned frame channel. The channel might have frames with multiple partitions
 * in it.
 */
public interface PartitionedReadableFrameChannel extends Closeable
{
  /**
   * Allows reading a particular partition in the channel
   * @param partitionNumber the partition to read
   * @return a ReadableFrameChannel for the partition queried
   */
  ReadableFrameChannel getReadableFrameChannel(int partitionNumber);

  /**
   * Releases any resources associated with this readable channel. After calling this, you should not call any other
   * methods on the channel.
   */
  @Override
  void close() throws IOException;
}
