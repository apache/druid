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

import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.SchemaPayload;
import org.apache.druid.segment.column.SegmentSchemaMetadata;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SegmentSchemaCacheTest
{
  @Test
  public void testCacheRealtimeSegmentSchema()
  {
    SegmentSchemaCache cache = new SegmentSchemaCache();

    RowSignature rowSignature = RowSignature.builder().add("cx", ColumnType.FLOAT).build();
    SegmentSchemaMetadata expected = new SegmentSchemaMetadata(new SchemaPayload(rowSignature), 20L);
    SegmentId id = SegmentId.dummy("ds");
    cache.addRealtimeSegmentSchema(id, rowSignature, 20);

    Assert.assertTrue(cache.isSchemaCached(id));
    Optional<SegmentSchemaMetadata> schema = cache.getSchemaForSegment(id);
    Assert.assertTrue(schema.isPresent());

    Assert.assertEquals(expected, schema.get());

    cache.segmentRemoved(id);
    Assert.assertFalse(cache.isSchemaCached(id));
  }

  @Test
  public void testCacheInTransitSMQResult()
  {
    SegmentSchemaCache cache = new SegmentSchemaCache();

    RowSignature rowSignature = RowSignature.builder().add("cx", ColumnType.FLOAT).build();
    SegmentSchemaMetadata expected = new SegmentSchemaMetadata(new SchemaPayload(rowSignature), 20L);
    SegmentId id = SegmentId.dummy("ds");
    cache.addInTransitSMQResult(id, rowSignature, 20);

    Assert.assertTrue(cache.isSchemaCached(id));
    Optional<SegmentSchemaMetadata> schema = cache.getSchemaForSegment(id);
    Assert.assertTrue(schema.isPresent());
    Assert.assertEquals(expected, schema.get());

    cache.markInTransitSMQResultPublished(id);

    schema = cache.getSchemaForSegment(id);
    Assert.assertTrue(schema.isPresent());
    Assert.assertEquals(expected, schema.get());

    cache.resetInTransitSMQResultPublishedOnDBPoll();

    Assert.assertFalse(cache.isSchemaCached(id));
    schema = cache.getSchemaForSegment(id);
    Assert.assertFalse(schema.isPresent());
  }

  @Test
  public void testCacheFinalizedSegmentSchema()
  {
    SegmentSchemaCache cache = new SegmentSchemaCache();

    RowSignature rowSignature = RowSignature.builder().add("cx", ColumnType.FLOAT).build();
    SegmentSchemaMetadata expected = new SegmentSchemaMetadata(new SchemaPayload(rowSignature), 20L);
    SegmentId id = SegmentId.dummy("ds");
    cache.addFinalizedSegmentSchema(0L, new SchemaPayload(rowSignature));
    ConcurrentMap<SegmentId, SegmentSchemaCache.SegmentStats> segmentStats = new ConcurrentHashMap<>();
    segmentStats.put(id, new SegmentSchemaCache.SegmentStats(0L, 20L));
    cache.updateFinalizedSegmentStatsReference(segmentStats);

    Assert.assertTrue(cache.isSchemaCached(id));
    Optional<SegmentSchemaMetadata> schema = cache.getSchemaForSegment(id);
    Assert.assertTrue(schema.isPresent());

    Assert.assertEquals(expected, schema.get());
  }
}
