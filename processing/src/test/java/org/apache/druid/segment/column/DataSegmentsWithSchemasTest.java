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

package org.apache.druid.segment.column;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.DataSegmentsWithSchemas;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SchemaPayloadPlus;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class DataSegmentsWithSchemasTest
{
  private ObjectMapper mapper = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws IOException
  {
    final DataSegment segment = new DataSegment(
        "foo",
        Intervals.of("2023-01-01/2023-01-02"),
        "2023-01-01",
        ImmutableMap.of("path", "a-1"),
        ImmutableList.of("dim1"),
        ImmutableList.of("m1"),
        new LinearShardSpec(0),
        9,
        100
    );

    SegmentSchemaMapping segmentSchemaMapping = new SegmentSchemaMapping(0);
    segmentSchemaMapping.addSchema(
        segment.getId(),
        new SchemaPayloadPlus(
            new SchemaPayload(
                RowSignature.builder().add("c", ColumnType.FLOAT).build()),
            20L
        ),
        "fp"
    );

    DataSegmentsWithSchemas dataSegmentsWithSchemas = new DataSegmentsWithSchemas(Collections.singleton(segment), segmentSchemaMapping);

    byte[] bytes = mapper.writeValueAsBytes(dataSegmentsWithSchemas);

    DataSegmentsWithSchemas deserialized = mapper.readValue(bytes, DataSegmentsWithSchemas.class);

    Assert.assertEquals(deserialized, dataSegmentsWithSchemas);
  }

  @Test
  public void testEquals()
  {
    final DataSegment segment = new DataSegment(
        "foo",
        Intervals.of("2023-01-01/2023-01-02"),
        "2023-01-01",
        ImmutableMap.of("path", "a-1"),
        ImmutableList.of("dim1"),
        ImmutableList.of("m1"),
        new LinearShardSpec(0),
        9,
        100
    );

    SegmentSchemaMapping segmentSchemaMapping = new SegmentSchemaMapping(0);
    segmentSchemaMapping.addSchema(
        segment.getId(),
        new SchemaPayloadPlus(
            new SchemaPayload(
                RowSignature.builder().add("c", ColumnType.FLOAT).build()),
            20L
        ),
        "fp"
    );

    DataSegmentsWithSchemas dataSegmentsWithSchemas = new DataSegmentsWithSchemas(Collections.singleton(segment), segmentSchemaMapping);

    DataSegmentsWithSchemas emptySegmentWithSchemas = new DataSegmentsWithSchemas(0);

    Assert.assertNotEquals(dataSegmentsWithSchemas, emptySegmentWithSchemas);
  }
}
