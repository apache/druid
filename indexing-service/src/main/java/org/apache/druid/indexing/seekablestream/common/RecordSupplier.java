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

package org.apache.druid.indexing.seekablestream.common;

import java.io.Closeable;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * The RecordSupplier interface is a wrapper for the incoming seekable data stream
 * (i.e. Kafka consumer)
 */
public interface RecordSupplier<T1, T2> extends Closeable
{
  void assign(Set<StreamPartition<T1>> partitions);

  void seek(StreamPartition<T1> partition, T2 sequenceNumber);

  void seekAfter(StreamPartition<T1> partition, T2 sequenceNumber);

  void seekToEarliest(Set<StreamPartition<T1>> partition);

  void seekToLatest(Set<StreamPartition<T1>> partition);

  Collection<StreamPartition<T1>> getAssignment();

  Record<T1, T2> poll(long timeout);

  T2 getLatestSequenceNumber(StreamPartition<T1> partition) throws TimeoutException;

  T2 getEarliestSequenceNumber(StreamPartition<T1> partition) throws TimeoutException;

  T2 position(StreamPartition<T1> partition);

  Set<T1> getPartitionIds(String streamName);


  @Override
  void close();
}
