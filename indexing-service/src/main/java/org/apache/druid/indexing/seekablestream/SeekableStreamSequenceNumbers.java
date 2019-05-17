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

package org.apache.druid.indexing.seekablestream;

import org.apache.druid.indexing.overlord.DataSourceMetadata;

import java.util.Map;

public interface SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType>
{
  /**
   * Returns the stream/topic name.
   */
  String getStream();

  /**
   * Returns a map of partitionId -> sequenceNumber.
   */
  Map<PartitionIdType, SequenceOffsetType> getPartitionSequenceNumberMap();

  /**
   * Merges this and the given other and returns the merged result.
   *
   * @see DataSourceMetadata#plus
   */
  SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> plus(
      SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> other
  );

  /**
   * Subtracts the given other from this and returns the result.
   *
   * @see DataSourceMetadata#minus
   */
  SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> minus(
      SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> other
  );
}
