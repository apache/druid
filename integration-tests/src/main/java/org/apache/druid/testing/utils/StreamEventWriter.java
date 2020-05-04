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

package org.apache.druid.testing.utils;


import java.io.Closeable;

/**
 * This interface is use to write test event data to the underlying stream (such as Kafka, Kinesis)
 * This can also be use with {@link StreamGenerator} to write particular set of test data
 */
public interface StreamEventWriter extends Closeable
{
  boolean isTransactionEnabled();

  void initTransaction();

  void commitTransaction();

  void write(String topic, byte[] event);

  /**
   * Flush pending writes on the underlying stream. This method is synchronous and waits until the flush completes.
   * Note that this method is not interruptible
   */
  void flush();

  /**
   * Close this writer. Any resource should be cleaned up when this method is called.
   * Implementations must call {@link #flush()} before closing the writer.
   */
  @Override
  void close();
}
