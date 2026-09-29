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
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

public class SegmentSchemaCacheTest
{
  @Test
  public void testCacheRealtimeSegmentSchema()
  {
    SegmentSchemaCache cache = new SegmentSchemaCache();

    RowSignature rowSignature = RowSignature.builder().add("cx", ColumnType.FLOAT).build();
    SchemaPayloadPlus expected = new SchemaPayloadPlus(new SchemaPayload(rowSignature), 20L);
    SegmentId id = SegmentId.dummy("ds");
    cache.addRealtimeSegmentSchema(id, expected);

    Assertions.assertTrue(cache.isSchemaCached(id));
    Optional<SchemaPayloadPlus> schema = cache.getSchemaForSegment(id);
    Assertions.assertTrue(schema.isPresent());

    Assertions.assertEquals(expected, schema.get());

    cache.segmentRemoved(id);
    Assertions.assertFalse(cache.isSchemaCached(id));
  }

  @Test
  public void testCacheTemporaryMetadataQueryResults()
  {
    SegmentSchemaCache cache = new SegmentSchemaCache();

    RowSignature rowSignature = RowSignature.builder().add("cx", ColumnType.FLOAT).build();
    SchemaPayloadPlus expected = new SchemaPayloadPlus(new SchemaPayload(rowSignature, Collections.emptyMap()), 20L);
    SegmentId id = SegmentId.dummy("ds");
    SegmentId id2 = SegmentId.dummy("ds2");

    // this call shouldn't result in any error
    cache.markSchemaPersisted(id);

    cache.addSchemaPendingBackfill(id, expected);
    cache.addSchemaPendingBackfill(id2, expected);

    Assertions.assertTrue(cache.isSchemaCached(id));
    Assertions.assertTrue(cache.isSchemaCached(id2));
    Optional<SchemaPayloadPlus> schema = cache.getSchemaForSegment(id);
    Assertions.assertTrue(schema.isPresent());
    Assertions.assertEquals(expected, schema.get());
    Optional<SchemaPayloadPlus> schema2 = cache.getSchemaForSegment(id);
    Assertions.assertTrue(schema2.isPresent());
    Assertions.assertEquals(expected, schema2.get());

    cache.markSchemaPersisted(id);
    cache.markSchemaPersisted(id2);

    schema = cache.getSchemaForSegment(id);
    Assertions.assertTrue(schema.isPresent());
    Assertions.assertEquals(expected, schema.get());

    // simulate call after segment polling

    ImmutableMap.Builder<SegmentId, SegmentMetadata> segmentMetadataBuilder = ImmutableMap.builder();
    segmentMetadataBuilder.put(id, new SegmentMetadata(5L, "fp"));

    ImmutableMap.Builder<String, SchemaPayload> schemaPayloadBuilder = ImmutableMap.builder();
    schemaPayloadBuilder.put("fp", new SchemaPayload(rowSignature));

    cache.resetSchemaForPublishedSegments(segmentMetadataBuilder.build(), schemaPayloadBuilder.build());

    Assertions.assertNull(cache.getTemporaryPublishedMetadataQueryResults(id));
    Assertions.assertNotNull(cache.getTemporaryPublishedMetadataQueryResults(id2));
    Assertions.assertTrue(cache.isSchemaCached(id));
    Assertions.assertTrue(cache.isSchemaCached(id2));
    schema = cache.getSchemaForSegment(id);
    Assertions.assertTrue(schema.isPresent());

    schema2 = cache.getSchemaForSegment(id2);
    Assertions.assertTrue(schema2.isPresent());
  }

  @Test
  public void testCacheFinalizedSegmentSchema()
  {
    SegmentSchemaCache cache = new SegmentSchemaCache();

    Assertions.assertFalse(cache.isInitialized());

    RowSignature rowSignature = RowSignature.builder().add("cx", ColumnType.FLOAT).build();
    SchemaPayloadPlus expected = new SchemaPayloadPlus(new SchemaPayload(rowSignature), 20L);
    SegmentId id = SegmentId.dummy("ds");

    ImmutableMap.Builder<String, SchemaPayload> schemaPayloadBuilder = new ImmutableMap.Builder<>();
    schemaPayloadBuilder.put("fp1", new SchemaPayload(rowSignature));

    ImmutableMap.Builder<SegmentId, SegmentMetadata> segmentMetadataBuilder = new ImmutableMap.Builder<>();
    segmentMetadataBuilder.put(id, new SegmentMetadata(20L, "fp1"));

    cache.resetSchemaForPublishedSegments(segmentMetadataBuilder.build(), schemaPayloadBuilder.build());

    Assertions.assertTrue(cache.isInitialized());
    Assertions.assertTrue(cache.isSchemaCached(id));
    Optional<SchemaPayloadPlus> schema = cache.getSchemaForSegment(id);
    Assertions.assertTrue(schema.isPresent());

    Assertions.assertEquals(expected, schema.get());
  }
}
