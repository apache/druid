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

package org.apache.druid.indexer.path;

import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.timeline.DataSegment;

import java.util.Set;

/**
 * Publishes segments to the metadata store. Used by stand-alone hadoop indexer
 * tasks to publish segments to the metadata store without going through the Overlord.
 */
public class SegmentMetadataPublisher
{
  private final IndexerMetadataStorageCoordinator storageCoordinator;

  public SegmentMetadataPublisher(IndexerMetadataStorageCoordinator storageCoordinator)
  {
    this.storageCoordinator = storageCoordinator;
  }

  public void publishSegments(Set<DataSegment> segments)
  {
    storageCoordinator.commitSegments(segments, null);
  }
}
