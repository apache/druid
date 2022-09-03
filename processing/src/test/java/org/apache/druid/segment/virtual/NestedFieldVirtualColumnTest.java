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

package org.apache.druid.segment.virtual;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.nested.NestedPathFinder;
import org.junit.Assert;
import org.junit.Test;

public class NestedFieldVirtualColumnTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    NestedFieldVirtualColumn there = new NestedFieldVirtualColumn("nested", "$.x.y.z", "v0", ColumnType.LONG);
    String json = JSON_MAPPER.writeValueAsString(there);
    NestedFieldVirtualColumn andBackAgain = JSON_MAPPER.readValue(json, NestedFieldVirtualColumn.class);
    Assert.assertEquals(there, andBackAgain);
  }

  @Test
  public void testSerdeArrayParts() throws JsonProcessingException
  {
    NestedFieldVirtualColumn there = new NestedFieldVirtualColumn("nested", "$.x.y.z[1]", "v0", ColumnType.LONG);
    String json = JSON_MAPPER.writeValueAsString(there);
    NestedFieldVirtualColumn andBackAgain = JSON_MAPPER.readValue(json, NestedFieldVirtualColumn.class);
    Assert.assertEquals(there, andBackAgain);
  }

  @Test
  public void testBothPathAndPartsDefined()
  {
    Assert.assertThrows(
        "Cannot define both 'path' and 'pathParts'",
        IllegalArgumentException.class,
        () -> new NestedFieldVirtualColumn(
            "nested",
            "v0",
            ColumnType.LONG,
            NestedPathFinder.parseJsonPath("$.x.y.z"),
            false,
            "$.x.y.z",
            false
        )
    );
  }

  @Test
  public void testNoPathAndPartsDefined()
  {
    Assert.assertThrows(
        "Must define exactly one of 'path' or 'pathParts'",
        IllegalArgumentException.class,
        () -> new NestedFieldVirtualColumn(
            "nested",
            "v0",
            ColumnType.LONG,
            null,
            null,
            null,
            null
        )
    );
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(NestedFieldVirtualColumn.class)
                  .withNonnullFields("columnName", "outputName")
                  .withIgnoredFields("hasNegativeArrayIndex")
                  .usingGetClass()
                  .verify();
  }
}
