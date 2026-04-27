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

package org.apache.druid.segment.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ScanTransformTest extends InitializedNullHandlingTest
{
  private static final long TIMESTAMP = DateTimes.of("2024-01-01").getMillis();

  private static InputRow makeRow(Object... kvPairs)
  {
    final java.util.LinkedHashMap<String, Object> event = new java.util.LinkedHashMap<>();
    final List<String> dimensions = new ArrayList<>();
    for (int i = 0; i < kvPairs.length; i += 2) {
      final String key = (String) kvPairs[i];
      event.put(key, kvPairs[i + 1]);
      if (!ColumnHolder.TIME_COLUMN_NAME.equals(key)) {
        dimensions.add(key);
      }
    }
    return new MapBasedInputRow(TIMESTAMP, dimensions, event);
  }

  private static ScanTransform makeUnnestTransform(String inputColumn, String outputName)
  {
    return new ScanTransform(
        outputName,
        new ExpressionVirtualColumn(outputName, "\"" + inputColumn + "\"", ColumnType.STRING, ExprMacroTable.nil()),
        null
    );
  }

  @Test
  public void testBasicUnnest()
  {
    final ScanTransform transform = makeUnnestTransform("tags", "tag");
    final InputRow input = makeRow("user", "alice", "tags", List.of("a", "b", "c"));

    final List<InputRow> result = transform.applyMultiRow(input);
    Assert.assertEquals(3, result.size());

    Assert.assertEquals("a", result.get(0).getRaw("tag"));
    Assert.assertEquals("alice", result.get(0).getRaw("user"));
    Assert.assertEquals(TIMESTAMP, result.get(0).getTimestampFromEpoch());

    Assert.assertEquals("b", result.get(1).getRaw("tag"));
    Assert.assertEquals("c", result.get(2).getRaw("tag"));
  }

  @Test
  public void testUnnestEmptyArray()
  {
    final ScanTransform transform = makeUnnestTransform("tags", "tag");
    final InputRow input = makeRow("user", "alice", "tags", List.of());

    final List<InputRow> result = transform.applyMultiRow(input);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("alice", result.get(0).getRaw("user"));
    Assert.assertNull(result.get(0).getRaw("tag"));
  }

  @Test
  public void testUnnestMissingColumn()
  {
    final ScanTransform transform = makeUnnestTransform("services", "svc");
    final InputRow input = makeRow("user", "alice", "host", "web-01");

    final List<InputRow> result = transform.applyMultiRow(input);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("alice", result.get(0).getRaw("user"));
    Assert.assertEquals("web-01", result.get(0).getRaw("host"));
    Assert.assertNull(result.get(0).getRaw("svc"));
  }

  @Test
  public void testUnnestSingleElement()
  {
    final ScanTransform transform = makeUnnestTransform("tags", "tag");
    final InputRow input = makeRow("user", "alice", "tags", List.of("only"));

    final List<InputRow> result = transform.applyMultiRow(input);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("only", result.get(0).getRaw("tag"));
  }

  @Test
  public void testUnnestScalarValue()
  {
    final ScanTransform transform = makeUnnestTransform("tags", "tag");
    final InputRow input = makeRow("user", "alice", "tags", "scalar");

    final List<InputRow> result = transform.applyMultiRow(input);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("scalar", result.get(0).getRaw("tag"));
  }

  @Test
  public void testUnnestArrayOfJsonObjects()
  {
    final ScanTransform transform = new ScanTransform(
        "item",
        new ExpressionVirtualColumn("item", "\"items\"", ColumnType.NESTED_DATA, ExprMacroTable.nil()),
        null
    );

    final List<Map<String, Object>> items = List.of(
        Map.of("product", "shirt", "price", 25),
        Map.of("product", "pants", "price", 40),
        Map.of("product", "hat", "price", 15)
    );
    final InputRow input = makeRow("user", "alice", "items", items);

    final List<InputRow> result = transform.applyMultiRow(input);
    Assert.assertEquals(3, result.size());

    final Object item0 = result.get(0).getRaw("item");
    Assert.assertNotNull(item0);
    Assert.assertTrue("Expected a Map, got " + item0.getClass(), item0 instanceof Map);
    Assert.assertEquals("shirt", ((Map<?, ?>) item0).get("product"));

    final Object item2 = result.get(2).getRaw("item");
    Assert.assertTrue(item2 instanceof Map);
    Assert.assertEquals("hat", ((Map<?, ?>) item2).get("product"));
  }

  @Test
  public void testUnnestNestedArrays()
  {
    final ScanTransform transform = new ScanTransform(
        "element",
        new ExpressionVirtualColumn("element", "\"data\"", ColumnType.NESTED_DATA, ExprMacroTable.nil()),
        null
    );

    final InputRow input = makeRow(
        "user", "alice",
        "data", List.of(List.of(1, 2), List.of(3))
    );

    final List<InputRow> result = transform.applyMultiRow(input);

    // One level of unnest only: [[1,2], [3]] -> [1,2] and [3]
    Assert.assertEquals(2, result.size());

    final Object elem0 = result.get(0).getRaw("element");
    Assert.assertNotNull(elem0);
    Assert.assertTrue("Expected a List, got " + elem0.getClass(), elem0 instanceof List);
    Assert.assertEquals(List.of(1, 2), elem0);

    final Object elem1 = result.get(1).getRaw("element");
    Assert.assertTrue("Expected a List, got " + elem1.getClass(), elem1 instanceof List);
    Assert.assertEquals(List.of(3), elem1);

    Assert.assertEquals("alice", result.get(0).getRaw("user"));
    Assert.assertEquals("alice", result.get(1).getRaw("user"));
  }

  @Test
  public void testTimestampPreservation()
  {
    final ScanTransform transform = makeUnnestTransform("tags", "tag");
    final InputRow input = makeRow("tags", List.of("a", "b"));

    final List<InputRow> result = transform.applyMultiRow(input);
    for (final InputRow row : result) {
      Assert.assertEquals(TIMESTAMP, row.getTimestampFromEpoch());
    }
  }

  @Test
  public void testWithUnnestFilter()
  {
    final ScanTransform transform = new ScanTransform(
        "tag",
        new ExpressionVirtualColumn("tag", "\"tags\"", ColumnType.STRING, ExprMacroTable.nil()),
        new SelectorDimFilter("tag", "b", null)
    );
    final InputRow input = makeRow("user", "alice", "tags", List.of("a", "b", "c"));

    final List<InputRow> result = transform.applyMultiRow(input);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("b", result.get(0).getRaw("tag"));
  }

  @Test
  public void testWithUnnestFilterDropsRowWhenNoValuesMatch()
  {
    final ScanTransform transform = new ScanTransform(
        "tag",
        new ExpressionVirtualColumn("tag", "\"tags\"", ColumnType.STRING, ExprMacroTable.nil()),
        new SelectorDimFilter("tag", "z", null)
    );
    final InputRow input = makeRow("user", "alice", "tags", List.of("a", "b", "c"));

    final List<InputRow> result = transform.applyMultiRow(input);
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testIsMultiRow()
  {
    final ScanTransform transform = makeUnnestTransform("tags", "tag");
    Assert.assertTrue(transform.isMultiRow());
    Assert.assertNull(transform.getRowFunction());
  }

  @Test
  public void testGetRequiredColumns()
  {
    final ScanTransform transform = makeUnnestTransform("tags", "tag");
    Assert.assertTrue(transform.getRequiredColumns().contains("tags"));
  }

  // --- Transformer integration tests ---

  @Test
  public void testTransformerWithSingleScanTransform()
  {
    final TransformSpec spec = new TransformSpec(
        null,
        List.of(makeUnnestTransform("tags", "tag"))
    );

    final Transformer transformer = spec.toTransformer();
    Assert.assertTrue(transformer.hasMultiRowTransform());

    final InputRow input = makeRow("user", "alice", "tags", List.of("x", "y"));
    final List<InputRow> result = transformer.transformToList(input);

    Assert.assertEquals(2, result.size());
    Assert.assertEquals("x", result.get(0).getRaw("tag"));
    Assert.assertEquals("y", result.get(1).getRaw("tag"));
  }

  @Test
  public void testTransformerWithMultipleScanTransforms()
  {
    final TransformSpec spec = new TransformSpec(
        null,
        List.of(
            makeUnnestTransform("tags", "tag"),
            makeUnnestTransform("colors", "color")
        )
    );

    final Transformer transformer = spec.toTransformer();
    Assert.assertTrue(transformer.hasMultiRowTransform());

    final InputRow input = makeRow(
        "user", "alice",
        "tags", List.of("a", "b"),
        "colors", List.of("red", "blue", "green")
    );
    final List<InputRow> result = transformer.transformToList(input);

    // 2 tags x 3 colors = 6 rows (cross join)
    Assert.assertEquals(6, result.size());
  }

  @Test
  public void testTransformerWithChainedScanTransformsFlattensNestedArrays()
  {
    // Two scan transforms: first unnests [[1,2],[3]] into [1,2] and [3],
    // second unnests those inner arrays into individual elements.
    final TransformSpec spec = new TransformSpec(
        null,
        List.of(
            new ScanTransform(
                "inner",
                new ExpressionVirtualColumn("inner", "\"data\"", ColumnType.NESTED_DATA, ExprMacroTable.nil()),
                null
            ),
            new ScanTransform(
                "val",
                new ExpressionVirtualColumn("val", "\"inner\"", ColumnType.LONG, ExprMacroTable.nil()),
                null
            )
        )
    );

    final Transformer transformer = spec.toTransformer();

    final InputRow input = makeRow(
        "user", "alice",
        "data", List.of(List.of(1, 2), List.of(3))
    );
    final List<InputRow> result = transformer.transformToList(input);

    // First unnest: [[1,2],[3]] -> [1,2], [3] (2 rows)
    // Second unnest: [1,2] -> 1, 2 and [3] -> 3 (3 rows total)
    Assert.assertEquals(3, result.size());

    final List<Object> values = new ArrayList<>();
    for (final InputRow row : result) {
      values.add(row.getRaw("val"));
      Assert.assertEquals("alice", row.getRaw("user"));
    }
    Assert.assertEquals(List.of(1, 2, 3), values);
  }

  @Test
  public void testTransformerWithExpressionAndScanTransforms()
  {
    final TransformSpec spec = new TransformSpec(
        null,
        List.of(
            new ExpressionTransform("upper_user", "upper(\"user\")", TestExprMacroTable.INSTANCE),
            makeUnnestTransform("tags", "tag")
        )
    );

    final Transformer transformer = spec.toTransformer();
    Assert.assertTrue(transformer.hasMultiRowTransform());

    final InputRow input = makeRow("user", "alice", "tags", List.of("a", "b"));
    final List<InputRow> result = transformer.transformToList(input);

    Assert.assertEquals(2, result.size());
    Assert.assertEquals("a", result.get(0).getRaw("tag"));
    Assert.assertEquals("b", result.get(1).getRaw("tag"));
    Assert.assertEquals("ALICE", result.get(0).getRaw("upper_user"));
    Assert.assertEquals("ALICE", result.get(1).getRaw("upper_user"));
  }

  @Test
  public void testTransformerWithScanTransformPreservesNonDimensionColumns()
  {
    final TransformSpec spec = new TransformSpec(
        null,
        List.of(makeUnnestTransform("tags", "tag"))
    );
    final Transformer transformer = spec.toTransformer();
    final InputRow input = new MapBasedInputRow(
        TIMESTAMP,
        List.of("user", "tags"),
        Map.of("user", "alice", "metricCount", 5L, "tags", List.of("a", "b"))
    );

    final List<InputRow> result = transformer.transformToList(input);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(5L, result.get(0).getRaw("metricCount"));
    Assert.assertEquals(5L, result.get(1).getRaw("metricCount"));
  }

  @Test
  public void testTransformerWithFilterAndScanTransform()
  {
    final TransformSpec spec = new TransformSpec(
        new SelectorDimFilter("user", "not_alice", null),
        List.of(makeUnnestTransform("tags", "tag"))
    );

    final Transformer transformer = spec.toTransformer();
    final InputRow input = makeRow("user", "alice", "tags", List.of("a", "b"));
    final List<InputRow> result = transformer.transformToList(input);
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testTransformerWithoutScanTransform()
  {
    final TransformSpec spec = new TransformSpec(null, null);
    final Transformer transformer = spec.toTransformer();
    Assert.assertFalse(transformer.hasMultiRowTransform());

    final InputRow input = makeRow("user", "alice");
    final List<InputRow> result = transformer.transformToList(input);
    Assert.assertEquals(1, result.size());
  }

  @Test
  public void testTransformerTransformToListWithNull()
  {
    final TransformSpec spec = new TransformSpec(null, null);
    final Transformer transformer = spec.toTransformer();
    Assert.assertTrue(transformer.transformToList(null).isEmpty());
  }

  // --- Serde tests ---

  @Test
  public void testSerde() throws Exception
  {
    final TransformSpec spec = new TransformSpec(
        null,
        List.of(
            new ScanTransform(
                "tag",
                new ExpressionVirtualColumn("tag", "\"tags\"", ColumnType.STRING, ExprMacroTable.nil()),
                new SelectorDimFilter("tag", "a", null)
            )
        )
    );

    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final String json = jsonMapper.writeValueAsString(spec);
    final TransformSpec deserialized = jsonMapper.readValue(json, TransformSpec.class);
    Assert.assertEquals(spec, deserialized);
  }

  @Test
  public void testSerdeWithMixedTransforms() throws Exception
  {
    final TransformSpec spec = new TransformSpec(
        null,
        List.of(
            new ExpressionTransform("upper_user", "upper(\"user\")", TestExprMacroTable.INSTANCE),
            new ScanTransform(
                "tag",
                new ExpressionVirtualColumn("tag", "\"tags\"", ColumnType.STRING, ExprMacroTable.nil()),
                null
            )
        )
    );

    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final String json = jsonMapper.writeValueAsString(spec);
    final TransformSpec deserialized = jsonMapper.readValue(json, TransformSpec.class);
    Assert.assertEquals(spec, deserialized);

    Assert.assertEquals(2, deserialized.getTransforms().size());
    Assert.assertFalse(deserialized.getTransforms().get(0).isMultiRow());
    Assert.assertTrue(deserialized.getTransforms().get(1).isMultiRow());
  }
}
