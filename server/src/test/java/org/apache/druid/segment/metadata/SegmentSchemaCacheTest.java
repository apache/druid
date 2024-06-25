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
  public void testCacheTemporaryMetadataQueryResults()
  {
    SegmentSchemaCache cache = new SegmentSchemaCache(new NoopServiceEmitter());

    RowSignature rowSignature = RowSignature.builder().add("cx", ColumnType.FLOAT).build();
    SchemaPayloadPlus expected = new SchemaPayloadPlus(new SchemaPayload(rowSignature, Collections.emptyMap()), 20L);
    SegmentId id = SegmentId.dummy("ds");
    SegmentId id2 = SegmentId.dummy("ds2");

    // this call shouldn't result in any error
    cache.markMetadataQueryResultPublished(id);

    cache.addTemporaryMetadataQueryResult(id, rowSignature, Collections.emptyMap(), 20);
    cache.addTemporaryMetadataQueryResult(id2, rowSignature, Collections.emptyMap(), 20);

    Assert.assertTrue(cache.isSchemaCached(id));
    Assert.assertTrue(cache.isSchemaCached(id2));
    Optional<SchemaPayloadPlus> schema = cache.getSchemaForSegment(id);
    Assert.assertTrue(schema.isPresent());
    Assert.assertEquals(expected, schema.get());
    Optional<SchemaPayloadPlus> schema2 = cache.getSchemaForSegment(id);
    Assert.assertTrue(schema2.isPresent());
    Assert.assertEquals(expected, schema2.get());

    cache.markMetadataQueryResultPublished(id);
    cache.markMetadataQueryResultPublished(id2);

    schema = cache.getSchemaForSegment(id);
    Assert.assertTrue(schema.isPresent());
    Assert.assertEquals(expected, schema.get());

    // simulate call after segment polling

    ImmutableMap.Builder<SegmentId, SegmentMetadata> segmentMetadataBuilder = ImmutableMap.builder();
    segmentMetadataBuilder.put(id, new SegmentMetadata(5L, "fp"));

    ImmutableMap.Builder<String, SchemaPayload> schemaPayloadBuilder = ImmutableMap.builder();
    schemaPayloadBuilder.put("fp", new SchemaPayload(rowSignature));

    SegmentSchemaCache.FinalizedSegmentSchemaInfo finalizedSegmentSchemaInfo =
        new SegmentSchemaCache.FinalizedSegmentSchemaInfo(segmentMetadataBuilder.build(), schemaPayloadBuilder.build());

    cache.updateFinalizedSegmentSchema(finalizedSegmentSchemaInfo);

    Assert.assertNull(cache.getTemporaryPublishedMetadataQueryResults(id));
    Assert.assertNotNull(cache.getTemporaryPublishedMetadataQueryResults(id2));
    Assert.assertTrue(cache.isSchemaCached(id));
    Assert.assertTrue(cache.isSchemaCached(id2));
    schema = cache.getSchemaForSegment(id);
    Assert.assertTrue(schema.isPresent());

    schema2 = cache.getSchemaForSegment(id2);
    Assert.assertTrue(schema2.isPresent());
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
