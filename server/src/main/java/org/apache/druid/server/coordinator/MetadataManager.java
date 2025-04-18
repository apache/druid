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

package org.apache.druid.server.coordinator;

import com.google.inject.Inject;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.timeline.DataSegment;

/**
 * Contains all metadata managers used by the Coordinator.
 */
public class MetadataManager
{
  private final AuditManager auditManager;
  private final CoordinatorConfigManager configManager;
  private final SegmentsMetadataManager segmentsMetadataManager;
  private final MetadataSupervisorManager metadataSupervisorManager;
  private final MetadataRuleManager metadataRuleManager;
  private final IndexerMetadataStorageCoordinator storageCoordinator;
  private final SegmentSchemaManager segmentSchemaManager;
  private final SegmentMetadataCache segmentMetadataCache;

  @Inject
  public MetadataManager(
      AuditManager auditManager,
      CoordinatorConfigManager configManager,
      SegmentsMetadataManager segmentsMetadataManager,
      MetadataSupervisorManager metadataSupervisorManager,
      MetadataRuleManager metadataRuleManager,
      IndexerMetadataStorageCoordinator storageCoordinator,
      SegmentSchemaManager segmentSchemaManager,
      SegmentMetadataCache segmentMetadataCache
  )
  {
    this.auditManager = auditManager;
    this.configManager = configManager;
    this.segmentsMetadataManager = segmentsMetadataManager;
    this.metadataSupervisorManager = metadataSupervisorManager;
    this.metadataRuleManager = metadataRuleManager;
    this.storageCoordinator = storageCoordinator;
    this.segmentSchemaManager = segmentSchemaManager;
    this.segmentMetadataCache = segmentMetadataCache;
  }

  public void onLeaderStart()
  {
    segmentsMetadataManager.startPollingDatabasePeriodically();
    segmentsMetadataManager.populateUsedFlagLastUpdatedAsync();
    segmentMetadataCache.becomeLeader();
    metadataRuleManager.start();
  }

  public void onLeaderStop()
  {
    metadataRuleManager.stop();
    segmentMetadataCache.stopBeingLeader();
    segmentsMetadataManager.stopPollingDatabasePeriodically();
    segmentsMetadataManager.stopAsyncUsedFlagLastUpdatedUpdate();
  }

  public boolean isStarted()
  {
    return segmentsMetadataManager.isPollingDatabasePeriodically();
  }

  public AuditManager audit()
  {
    return auditManager;
  }

  public CoordinatorConfigManager configs()
  {
    return configManager;
  }

  public MetadataSupervisorManager supervisors()
  {
    return metadataSupervisorManager;
  }

  public MetadataRuleManager rules()
  {
    return metadataRuleManager;
  }

  public SegmentsMetadataManager segments()
  {
    return segmentsMetadataManager;
  }

  public IndexerMetadataStorageCoordinator indexer()
  {
    return storageCoordinator;
  }

  public SegmentSchemaManager schemas()
  {
    return segmentSchemaManager;
  }

  /**
   * Returns an iterable to go over all segments in all data sources. The order in which segments are iterated is
   * unspecified. Note: the iteration may not be as trivially cheap as, for example, iteration over an ArrayList. Try
   * (to some reasonable extent) to organize the code so that it iterates the returned iterable only once rather than
   * several times.
   */
  public Iterable<DataSegment> iterateAllUsedSegments()
  {
    return segments().getDataSourceSnapshot().iterateAllUsedSegmentsInSnapshot();
  }

}
