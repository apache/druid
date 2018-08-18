/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.realtime.appenderator;

import io.druid.indexing.overlord.SegmentPublishResult;
import io.druid.timeline.DataSegment;

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
  SegmentPublishResult publishSegments(
      Set<DataSegment> segments,
      @Nullable Object commitMetadata
  ) throws IOException;
}
