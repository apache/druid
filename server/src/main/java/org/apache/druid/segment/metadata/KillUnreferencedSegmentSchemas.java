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

package org.apache.druid.segment.metadata;

import com.google.inject.Inject;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.SegmentsMetadataManager;

import java.util.List;

/**
 * This class deals with cleaning schema which is not referenced by any used segment.
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
 *    <li> Between ops 1 and 3b: In other words, we might end up with a segment that points to a schema that has just been marked as unused. This can be repaired by the coordinator duty. </li>
 *    <li> Between 2 and 3c: This can be handled. Either 2 will fail to update any rows (good case) or 3c will fail to update any rows and thus return 0 (bad case). In the bad case, we need to recreate the schema, same as step 3a. </li>
 *    </ol>
 * </p>
 */
@LazySingleton
public class KillUnreferencedSegmentSchemas
{
  private static final EmittingLogger log = new EmittingLogger(KillUnreferencedSegmentSchemas.class);
  private final SegmentSchemaManager segmentSchemaManager;
  private final SegmentsMetadataManager metadataManager;

  @Inject
  public KillUnreferencedSegmentSchemas(
      SegmentSchemaManager segmentSchemaManager,
      SegmentsMetadataManager metadataManager
  )
  {
    this.segmentSchemaManager = segmentSchemaManager;
    this.metadataManager = metadataManager;
  }

  public int cleanup(long timestamp)
  {
    // 1: Identify unreferenced schema and mark them as unused. These will get deleted after a fixed period.
    int unused = segmentSchemaManager.identifyAndMarkSchemaUnused();
    log.info("Identified [%s] unreferenced schema. Marking them as unused.", unused);

    // 2 (repair step): Identify unused schema which are still referenced by segments, make them used.
    // This case would arise when segment is associated with a schema which turned unused by the previous statement
    // or the previous run of this duty.
    List<Long> schemaIdsToUpdate = segmentSchemaManager.identifyReferencedUnusedSchema();
    if (schemaIdsToUpdate.size() > 0) {
      segmentSchemaManager.markSchemaUsed(schemaIdsToUpdate);
      log.info("Identified [%s] unused schemas still referenced by used segments. Marking them as used.", schemaIdsToUpdate.size());
    }

    // 3: Delete unused schema older than {@code timestamp}.
    int deleted = segmentSchemaManager.deleteSchemasOlderThan(timestamp);

    if (deleted > 0) {
      log.info("Deleted [%s] schemas.", deleted);
      // Reset latest segment schema Id to trigger full schema refresh.
      metadataManager.resetLatestSchemaId();
    }
    return deleted;
  }
}
