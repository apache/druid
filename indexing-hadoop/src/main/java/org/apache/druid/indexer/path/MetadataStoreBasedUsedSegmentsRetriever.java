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

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.Collection;
import java.util.List;

/**
 */
public class MetadataStoreBasedUsedSegmentsRetriever implements UsedSegmentsRetriever
{
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;

  @Inject
  public MetadataStoreBasedUsedSegmentsRetriever(IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator)
  {
    this.indexerMetadataStorageCoordinator = Preconditions.checkNotNull(
        indexerMetadataStorageCoordinator,
        "null indexerMetadataStorageCoordinator"
    );
  }

  @Override
  public Collection<DataSegment> retrieveUsedSegmentsForIntervals(
      String dataSource,
      List<Interval> intervals,
      Segments visibility
  )
  {
    return indexerMetadataStorageCoordinator.retrieveUsedSegmentsForIntervals(dataSource, intervals, visibility);
  }
}
