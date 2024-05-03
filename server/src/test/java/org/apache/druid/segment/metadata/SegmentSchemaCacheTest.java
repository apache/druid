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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SchemaPayloadPlus;
import org.apache.druid.segment.SegmentMetadata;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

public class SegmentSchemaCacheTest
{
  @Test
  public void testCacheRealtimeSegmentSchema()
  {
    SegmentSchemaCache cache = new SegmentSchemaCache(new NoopServiceEmitter());

    RowSignature rowSignature = RowSignature.builder().add("cx", ColumnType.FLOAT).build();
    SchemaPayloadPlus expected = new SchemaPayloadPlus(new SchemaPayload(rowSignature), 20L);
    SegmentId id = SegmentId.dummy("ds");
    cache.addRealtimeSegmentSchema(id, rowSignature, 20);

    Assert.assertTrue(cache.isSchemaCached(id));
    Optional<SchemaPayloadPlus> schema = cache.getSchemaForSegment(id);
    Assert.assertTrue(schema.isPresent());

    Assert.assertEquals(expected, schema.get());

    cache.segmentRemoved(id);
    Assert.assertFalse(cache.isSchemaCached(id));
  }

  @Test
  public void testCacheInTransitSMQResult()
  {
    SegmentSchemaCache cache = new SegmentSchemaCache(new NoopServiceEmitter());

    RowSignature rowSignature = RowSignature.builder().add("cx", ColumnType.FLOAT).build();
    SchemaPayloadPlus expected = new SchemaPayloadPlus(new SchemaPayload(rowSignature, Collections.emptyMap()), 20L);
    SegmentId id = SegmentId.dummy("ds");
    cache.addInTransitSMQResult(id, rowSignature, Collections.emptyMap(), 20);

    Assert.assertTrue(cache.isSchemaCached(id));
    Optional<SchemaPayloadPlus> schema = cache.getSchemaForSegment(id);
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
    SegmentSchemaCache cache = new SegmentSchemaCache(new NoopServiceEmitter());

    Assert.assertFalse(cache.isInitialized());

    RowSignature rowSignature = RowSignature.builder().add("cx", ColumnType.FLOAT).build();
    SchemaPayloadPlus expected = new SchemaPayloadPlus(new SchemaPayload(rowSignature), 20L);
    SegmentId id = SegmentId.dummy("ds");

    ImmutableMap.Builder<String, SchemaPayload> schemaPayloadBuilder = new ImmutableMap.Builder<>();
    schemaPayloadBuilder.put("fp1", new SchemaPayload(rowSignature));

    ImmutableMap.Builder<SegmentId, SegmentMetadata> segmentMetadataBuilder = new ImmutableMap.Builder<>();
    segmentMetadataBuilder.put(id, new SegmentMetadata(20L, "fp1"));

    cache.updateFinalizedSegmentSchema(new SegmentSchemaCache.FinalizedSegmentSchemaInfo(segmentMetadataBuilder.build(), schemaPayloadBuilder.build()));

    Assert.assertTrue(cache.isInitialized());
    Assert.assertTrue(cache.isSchemaCached(id));
    Optional<SchemaPayloadPlus> schema = cache.getSchemaForSegment(id);
    Assert.assertTrue(schema.isPresent());

    Assert.assertEquals(expected, schema.get());
  }
}
