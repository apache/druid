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
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class NestedObjectVirtualColumnTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    Map<String, NestedObjectVirtualColumn.TypedExpression> keyExprMap = ImmutableMap.of(
        "computed", new NestedObjectVirtualColumn.TypedExpression("col1 + col2", ColumnType.LONG),
        "concatenated", new NestedObjectVirtualColumn.TypedExpression("concat(col3, '_suffix')", ColumnType.STRING)
    );
    NestedObjectVirtualColumn there = new NestedObjectVirtualColumn(
        "computed_obj",
        keyExprMap,
        TestExprMacroTable.INSTANCE
    );
    String json = JSON_MAPPER.writeValueAsString(there);
    NestedObjectVirtualColumn andBackAgain = JSON_MAPPER.readValue(json, NestedObjectVirtualColumn.class);
    Assert.assertEquals(there, andBackAgain);
  }

  @Test
  public void testGetKeyExprMap()
  {
    Map<String, NestedObjectVirtualColumn.TypedExpression> keyExprMap = new HashMap<>();
    keyExprMap.put("key1", new NestedObjectVirtualColumn.TypedExpression("expr1", ColumnType.STRING));
    keyExprMap.put("key2", new NestedObjectVirtualColumn.TypedExpression("expr2", ColumnType.DOUBLE));

    NestedObjectVirtualColumn column = new NestedObjectVirtualColumn(
        "test_obj",
        keyExprMap,
        TestExprMacroTable.INSTANCE
    );
    Assert.assertEquals(keyExprMap, column.getKeyExprMap());
  }

  @Test
  public void testToString()
  {
    Map<String, NestedObjectVirtualColumn.TypedExpression> keyExprMap = ImmutableMap.of(
        "key1", new NestedObjectVirtualColumn.TypedExpression("col1", ColumnType.STRING)
    );
    NestedObjectVirtualColumn column = new NestedObjectVirtualColumn(
        "test_obj",
        keyExprMap,
        TestExprMacroTable.INSTANCE
    );
    String result = column.toString();
    Assert.assertTrue(result.startsWith("NestedObjectVirtualColumn{"));
    Assert.assertTrue(result.contains("name='test_obj'"));
    Assert.assertTrue(result.contains("object="));
  }

  @Test
  public void testTypedExpressionSerde() throws JsonProcessingException
  {
    NestedObjectVirtualColumn.TypedExpression there = new NestedObjectVirtualColumn.TypedExpression(
        "col1 + col2",
        ColumnType.LONG
    );
    String json = JSON_MAPPER.writeValueAsString(there);
    NestedObjectVirtualColumn.TypedExpression andBackAgain = JSON_MAPPER.readValue(
        json,
        NestedObjectVirtualColumn.TypedExpression.class
    );
    Assert.assertEquals(there, andBackAgain);
  }

  @Test
  public void testTypedExpressionGetters()
  {
    NestedObjectVirtualColumn.TypedExpression typedExpr = new NestedObjectVirtualColumn.TypedExpression(
        "test_expression",
        ColumnType.DOUBLE
    );
    Assert.assertEquals("test_expression", typedExpr.getExpression());
    Assert.assertEquals(ColumnType.DOUBLE, typedExpr.getType());
  }

  @Test
  public void testTypedExpressionEqualsAndHashcode()
  {
    EqualsVerifier.forClass(NestedObjectVirtualColumn.TypedExpression.class)
                  .withNonnullFields("expression", "type")
                  .usingGetClass()
                  .verify();
  }
}
