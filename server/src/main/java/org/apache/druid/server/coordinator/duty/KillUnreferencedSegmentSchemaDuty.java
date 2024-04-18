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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.stats.Stats;
import org.joda.time.DateTime;

import java.util.List;

/**
 * Coordinator duty to clean up segment schema which are not referenced by any used segment.
 * <p>
 * <ol>
 * <li>If a schema is not referenced, UPDATE schemas SET used = false, used_status_last_updated = now</li>
 * <li>DELETE FROM schemas WHERE used = false AND used_status_last_updated < 6 hours ago</li>
 * <li>When creating a new segment, try to find schema for the fingerprint of the segment.</li>
 *    <ol type="a">
 *    <li> If no record found, create a new one.</li>
 *    <li> If record found which has used = true, reuse this schema_id.</li>
 *    <li> If record found which has used = false, UPDATE SET used = true, used_status_last_updated = now</li>
 *    </ol>
 * </ol>
 * </p>
 * <p>
 * Possible race conditions:
 *    <ol type="a">
 *    <li> Between ops 1 and 3b: In other words, we might end up with a segment that points to a schema that has just been marked as unused. This is repaired by the coordinator duty.</li>
 *    <li> Between 2 and 3c: This can be handled. Either 2 will fail to update any rows (good case) or 3c will fail to update any rows (bad case). In the bad case, we need to recreate the schema, same as step 3a. </li>
 *    </ol>
 * </p>
 */
public class KillUnreferencedSegmentSchemaDuty extends MetadataCleanupDuty
{
  private static final Logger log = new Logger(KillUnreferencedSegmentSchemaDuty.class);
  private final SegmentSchemaManager segmentSchemaManager;
  private final SegmentsMetadataManager segmentsMetadataManager;

  public KillUnreferencedSegmentSchemaDuty(
      DruidCoordinatorConfig config,
      SegmentSchemaManager segmentSchemaManager,
      SegmentsMetadataManager segmentsMetadataManager
  )
  {
    super(
        "segmentSchema",
        "druid.coordinator.kill.segmentSchema",
        config.isSegmentSchemaKillEnabled(),
        config.getSegmentSchemaKillPeriod(),
        config.getSegmentSchemaKillDurationToRetain(),
        Stats.Kill.RULES,
        config
    );
    this.segmentSchemaManager = segmentSchemaManager;
    this.segmentsMetadataManager = segmentsMetadataManager;
  }

  @Override
  protected int cleanupEntriesCreatedBefore(DateTime minCreatedTime)
  {
    log.info("MinCreatedTime is [%s], currentTime is [%s]", minCreatedTime, DateTimes.nowUtc().toString());
    // 1: Identify unreferenced schema and mark them as unused. These will get deleted after a fixed period.
    int unused = segmentSchemaManager.markUnreferencedSchemasAsUnused();
    log.info("Identified [%s] unreferenced schema and marking them as unused.", unused);

    // 2 (repair step): Identify unused schema which are still referenced by segments, make them used.
    // This case would arise when segment is associated with a schema which turned unused by the previous statement
    // or the previous run of this duty.
    List<Long> schemaIdsToUpdate = segmentSchemaManager.findReferencedSchemaIdsMarkedAsUnused();
    if (schemaIdsToUpdate.size() > 0) {
      segmentSchemaManager.markSchemaIdsAsUsed(schemaIdsToUpdate);
      log.info("Identified [%s] unused schemas still referenced by used segments and marking them as used.", schemaIdsToUpdate.size());
    }

    // 3: Delete unused schema older than timestamp.
    int deleted = segmentSchemaManager.deleteSchemasOlderThan(minCreatedTime.getMillis());

    if (deleted > 0) {
      log.info("Deleted [%s] schemas older than [%s].", deleted, minCreatedTime);
      // Reset latest segment schema Id to trigger full schema refresh.
      segmentsMetadataManager.refreshSegmentSchema();
    }
    return deleted;
  }
}
