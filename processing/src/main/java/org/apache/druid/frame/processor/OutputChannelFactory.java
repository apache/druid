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

package org.apache.druid.frame.processor;

import java.io.IOException;

/**
 * Factory for generating channel pairs for output data from processors.
 */
public interface OutputChannelFactory
{
  /**
   * Create a channel pair tagged with a particular partition number.
   */
  OutputChannel openChannel(int partitionNumber) throws IOException;

  /**
   * Create a channel pair tagged with a particular name and a flag to delete the channel data after its read.
   */
  PartitionedOutputChannel openPartitionedChannel(String name, boolean deleteAfterRead) throws IOException;

  /**
   * Create a non-writable, always-empty channel pair tagged with a particular partition number.
   *
   * Calling {@link OutputChannel#getWritableChannel()} on this nil channel pair will result in an error. Calling
   * {@link OutputChannel#getReadableChannel()} will return an empty channel.
   */
  OutputChannel openNilChannel(int partitionNumber);

  /**
   * Whether this factory creates buffered channels. Buffered channel readers
   * (from {@link OutputChannel#getReadableChannel()}) will not work properly until the writer is closed.
   * Unbuffered readers work immediately, even while the writer is open.
   */
  boolean isBuffered();
}
