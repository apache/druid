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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.last.StringLastAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class MinimalSegmentSchemasTest
{
  static {
    NullHandling.initializeForTests();
  }

  @Test
  public void testSerde() throws IOException
  {
    RowSignature rowSignature = RowSignature.builder().add("c", ColumnType.FLOAT).build();

    StringLastAggregatorFactory factory = new StringLastAggregatorFactory("billy", "nilly", null, 20);
    SchemaPayload payload = new SchemaPayload(rowSignature, Collections.singletonMap("twosum", factory));

    MinimalSegmentSchemas minimalSegmentSchemas = new MinimalSegmentSchemas(
        Collections.singletonMap("id1", new MinimalSegmentSchemas.SegmentStats(20L, "fp1")),
        Collections.singletonMap("fp1", payload)
    );

    ObjectMapper mapper = TestHelper.makeJsonMapper();
    byte[] bytes = mapper.writeValueAsBytes(minimalSegmentSchemas);
    MinimalSegmentSchemas deserialized = mapper.readValue(bytes, MinimalSegmentSchemas.class);

    Assert.assertEquals(minimalSegmentSchemas, deserialized);

    MinimalSegmentSchemas copy = new MinimalSegmentSchemas();
    copy.add(minimalSegmentSchemas);

    Assert.assertEquals(minimalSegmentSchemas, copy);

    MinimalSegmentSchemas copy2 = new MinimalSegmentSchemas();
    copy2.addSchema("id1", "fp1", 20L, payload);

    Assert.assertEquals(minimalSegmentSchemas, copy2);
  }
}
