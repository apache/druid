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

package org.apache.druid.data.input.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.segment.transform.TransformingInputEntityReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class NestedColumnParquetReaderTest extends BaseParquetReaderTest
{
  @Test
  public void testNestedColumnTransformsNestedTestFile() throws IOException
  {
    final String file = "example/flattening/test_nested_1.parquet";
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(
            ImmutableList.of(
                new AutoTypeColumnSchema("nestedData", null),
                new AutoTypeColumnSchema("t_nestedData_listDim", null),
                new StringDimensionSchema("t_nestedData_listDim_string"),
                new StringDimensionSchema("t_nestedData_dim2"),
                new LongDimensionSchema("t_nestedData_dim3"),
                new LongDimensionSchema("t_nestedData_metric2"),
                new StringDimensionSchema("t_nestedData_listDim1"),
                new StringDimensionSchema("t_nestedData_listDim2")
            )
        ),
        ColumnsFilter.all()
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(true, ImmutableList.of());
    InputEntityReader reader = createReader(
        file,
        schema,
        flattenSpec
    );
    TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExpressionTransform("t_nestedData_dim2", "json_value(nestedData, '$.dim2')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("t_nestedData_dim3", "json_value(nestedData, '$.dim3')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("t_nestedData_metric2", "json_value(nestedData, '$.metric2')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("t_nestedData_listDim", "json_query(nestedData, '$.listDim')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("t_nestedData_listDim_string", "json_query(nestedData, '$.listDim')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("t_nestedData_listDim_1", "json_value(nestedData, '$.listDim[0]')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("t_nestedData_listDim_2", "json_query(nestedData, '$.listDim[1]')", TestExprMacroTable.INSTANCE)
        )
    );
    TransformingInputEntityReader transformingReader = new TransformingInputEntityReader(
        reader,
        transformSpec.toTransformer()
    );

    List<InputRow> rows = readAllRows(transformingReader);
    Assert.assertEquals(FlattenSpecParquetInputTest.TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals(1L, rows.get(0).getRaw("t_nestedData_dim3"));
    Assert.assertEquals("d2v1", rows.get(0).getRaw("t_nestedData_dim2"));
    Assert.assertEquals(ImmutableList.of("listDim1v1", "listDim1v2"), rows.get(0).getRaw("t_nestedData_listDim"));
    Assert.assertEquals(ImmutableList.of("listDim1v1", "listDim1v2"), rows.get(0).getDimension("t_nestedData_listDim_string"));
    Assert.assertEquals("listDim1v1", rows.get(0).getRaw("t_nestedData_listDim_1"));
    Assert.assertEquals("listDim1v2", rows.get(0).getRaw("t_nestedData_listDim_2"));
    Assert.assertEquals(2L, rows.get(0).getRaw("t_nestedData_metric2"));
  }

  @Test
  public void testNestedColumnTransformsNestedNullableListFile() throws IOException
  {
    final String file = "example/flattening/nullable_list.snappy.parquet";
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(
            ImmutableList.of(
                new AutoTypeColumnSchema("a1", null),
                new AutoTypeColumnSchema("a2", null),
                new AutoTypeColumnSchema("t_a2", null),
                new AutoTypeColumnSchema("t_a1_b1", null),
                new LongDimensionSchema("t_a1_b1_c1"),
                new LongDimensionSchema("t_e2_0_b1"),
                new LongDimensionSchema("tt_a2_0_b1")
            )
        ),
        ColumnsFilter.all()
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(true, ImmutableList.of());
    InputEntityReader reader = createReader(
        file,
        schema,
        flattenSpec
    );
    TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExpressionTransform("t_a1_b1", "json_query(a1, '$.b1')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("t_a2", "json_query(a2, '$')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("t_a1_b1_c1", "json_value(a1, '$.b1.c1')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("t_a2_0_b1", "json_value(a2, '$[0].b1')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("t_a2_0", "json_query(a2, '$[0]')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("t_a2_1_b1", "json_value(a2, '$[1].b1')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("tt_a2_0_b1", "json_value(json_query(a2, '$'), '$[0].b1')", TestExprMacroTable.INSTANCE)
        )
    );
    TransformingInputEntityReader transformingReader = new TransformingInputEntityReader(
        reader,
        transformSpec.toTransformer()
    );

    List<InputRow> rows = readAllRows(transformingReader);

    Assert.assertEquals("2022-02-01T00:00:00.000Z", rows.get(0).getTimestamp().toString());
    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("b1", 1L, "b2", 2L), ImmutableMap.of("b1", 1L, "b2", 2L)
        ),
        rows.get(0).getRaw("a2")
    );
    // expression turns List into Object[]
    Assert.assertArrayEquals(
        new Object[]{
            ImmutableMap.of("b1", 1L, "b2", 2L), ImmutableMap.of("b1", 1L, "b2", 2L)
        },
        (Object[]) rows.get(0).getRaw("t_a2")
    );
    Assert.assertEquals(ImmutableMap.of("c1", 1L, "c2", 2L), rows.get(0).getRaw("t_a1_b1"));
    Assert.assertEquals(ImmutableMap.of("b1", 1L, "b2", 2L), rows.get(0).getRaw("t_a2_0"));
    Assert.assertEquals(1L, rows.get(0).getRaw("t_a1_b1_c1"));
    Assert.assertEquals(1L, rows.get(0).getRaw("t_a2_0_b1"));
    Assert.assertEquals(1L, rows.get(0).getRaw("t_a2_1_b1"));
    Assert.assertEquals(1L, rows.get(0).getRaw("tt_a2_0_b1"));
  }

  @Test
  public void testNestedColumnSchemalessNestedTestFileNoNested() throws IOException
  {
    final String file = "example/flattening/test_nested_1.parquet";
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", null),
        DimensionsSpec.builder().useSchemaDiscovery(false).build(),
        ColumnsFilter.all(),
        null
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(true, ImmutableList.of());
    InputEntityReader reader = createReader(
        file,
        schema,
        flattenSpec
    );

    List<InputRow> rows = readAllRows(reader);
    Assert.assertEquals(ImmutableList.of("dim1", "metric1"), rows.get(0).getDimensions());
    Assert.assertEquals(FlattenSpecParquetInputTest.TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals(ImmutableList.of("d1v1"), rows.get(0).getDimension("dim1"));
    Assert.assertEquals("d1v1", rows.get(0).getRaw("dim1"));
    Assert.assertEquals(ImmutableList.of("1"), rows.get(0).getDimension("metric1"));
    Assert.assertEquals(1, rows.get(0).getRaw("metric1"));
    Assert.assertEquals(1, rows.get(0).getMetric("metric1"));
    // can still read even if it doesn't get reported as a dimension
    Assert.assertEquals(
        ImmutableMap.of(
            "listDim", ImmutableList.of("listDim1v1", "listDim1v2"),
            "dim3", 1,
            "dim2", "d2v1",
            "metric2", 2
        ),
        rows.get(0).getRaw("nestedData")
    );
  }

  @Test
  public void testNestedColumnSchemalessNestedTestFile() throws IOException
  {
    final String file = "example/flattening/test_nested_1.parquet";
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", null),
        DimensionsSpec.builder().useSchemaDiscovery(true).build(),
        ColumnsFilter.all(),
        null
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(true, ImmutableList.of());
    InputEntityReader reader = createReader(
        file,
        schema,
        flattenSpec
    );

    List<InputRow> rows = readAllRows(reader);
    Assert.assertEquals(ImmutableList.of("nestedData", "dim1", "metric1"), rows.get(0).getDimensions());
    Assert.assertEquals(FlattenSpecParquetInputTest.TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals(ImmutableList.of("d1v1"), rows.get(0).getDimension("dim1"));
    Assert.assertEquals("d1v1", rows.get(0).getRaw("dim1"));
    Assert.assertEquals(ImmutableList.of("1"), rows.get(0).getDimension("metric1"));
    Assert.assertEquals(1, rows.get(0).getRaw("metric1"));
    Assert.assertEquals(1, rows.get(0).getMetric("metric1"));
    Assert.assertEquals(
        ImmutableMap.of(
            "listDim", ImmutableList.of("listDim1v1", "listDim1v2"),
            "dim3", 1,
            "dim2", "d2v1",
            "metric2", 2
        ),
        rows.get(0).getRaw("nestedData")
    );
  }
}
