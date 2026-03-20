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
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class NestedMergeVirtualColumnTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    NestedMergeVirtualColumn there = new NestedMergeVirtualColumn(
        "merged",
        ImmutableList.of("col1", "col2", "col3"),
        TestExprMacroTable.INSTANCE
    );
    String json = JSON_MAPPER.writeValueAsString(there);
    NestedMergeVirtualColumn andBackAgain = JSON_MAPPER.readValue(json, NestedMergeVirtualColumn.class);
    Assert.assertEquals(there, andBackAgain);
  }

  @Test
  public void testSerdeWithSingleColumn() throws JsonProcessingException
  {
    NestedMergeVirtualColumn there = new NestedMergeVirtualColumn(
        "merged",
        ImmutableList.of("singleCol"),
        TestExprMacroTable.INSTANCE
    );
    String json = JSON_MAPPER.writeValueAsString(there);
    NestedMergeVirtualColumn andBackAgain = JSON_MAPPER.readValue(json, NestedMergeVirtualColumn.class);
    Assert.assertEquals(there, andBackAgain);
  }

  @Test
  public void testGetColumns()
  {
    NestedMergeVirtualColumn column = new NestedMergeVirtualColumn(
        "merged",
        Arrays.asList("col1", "col2", "col3"),
        TestExprMacroTable.INSTANCE
    );
    Assert.assertEquals(Arrays.asList("col1", "col2", "col3"), column.getColumns());
  }

  @Test
  public void testEquivalence()
  {
    NestedMergeVirtualColumn v1 = new NestedMergeVirtualColumn(
        "merged1",
        Arrays.asList("col1", "col2"),
        TestExprMacroTable.INSTANCE
    );
    NestedMergeVirtualColumn v2 = new NestedMergeVirtualColumn(
        "merged2",
        Arrays.asList("col1", "col2"),
        TestExprMacroTable.INSTANCE
    );
    NestedMergeVirtualColumn v3 = new NestedMergeVirtualColumn(
        "merged1",
        Arrays.asList("col1", "col3"),
        TestExprMacroTable.INSTANCE
    );

    Assert.assertNotEquals(v1, v2);
    Assert.assertEquals(v1.getEquivalanceKey(), v2.getEquivalanceKey());
    Assert.assertNotEquals(v1, v3);
    Assert.assertNotEquals(v1.getEquivalanceKey(), v3.getEquivalanceKey());
  }

  @Test
  public void testToString()
  {
    NestedMergeVirtualColumn column = new NestedMergeVirtualColumn(
        "merged",
        Arrays.asList("col1", "col2"),
        TestExprMacroTable.INSTANCE
    );
    String expected = "NestedMergeVirtualColumn{name='merged', columns=[col1, col2]}";
    Assert.assertEquals(expected, column.toString());
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(NestedMergeVirtualColumn.class)
                  .withNonnullFields("columns")
                  .usingGetClass()
                  .verify();
  }
}
