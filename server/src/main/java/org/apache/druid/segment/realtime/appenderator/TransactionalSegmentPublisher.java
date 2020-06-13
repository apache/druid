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

package org.apache.druid.segment.realtime.appenderator;

import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Set;

public interface TransactionalSegmentPublisher
{
  /**
   * Publish segments, along with some commit metadata, in a single transaction.
   *
   * @return publish result that indicates if segments were published or not. If it is unclear
   * if the segments were published or not, this method must throw an exception. The behavior is similar to
   * IndexerSQLMetadataStorageCoordinator's announceHistoricalSegments.
   *
   * @throws IOException if there was an I/O error when publishing
   * @throws RuntimeException if we cannot tell if the segments were published or not, for some other reason
   */
  SegmentPublishResult publishAnnotatedSegments(
      @Nullable Set<DataSegment> segmentsToBeOverwritten,
      Set<DataSegment> segmentsToPublish,
      @Nullable Object commitMetadata
  ) throws IOException;

  default SegmentPublishResult publishSegments(
      @Nullable Set<DataSegment> segmentsToBeOverwritten,
      Set<DataSegment> segmentsToPublish,
      @Nullable Object commitMetadata
  )
      throws IOException
  {
    return publishAnnotatedSegments(
        segmentsToBeOverwritten,
        SegmentPublisherHelper.annotateShardSpec(segmentsToPublish),
        commitMetadata
    );
  }

  /**
   * @return true if this publisher has action to take when publishing with an empty segment set.
   *         The publisher used by the seekable stream tasks is an example where this is true.
   */
  default boolean supportsEmptyPublish()
  {
    return false;
  }
}
