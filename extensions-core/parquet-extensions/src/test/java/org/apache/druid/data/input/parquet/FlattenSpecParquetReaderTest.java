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
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Duplicate of {@link FlattenSpecParquetInputTest} but for {@link DruidParquetReader} instead of Hadoop
 */
public class FlattenSpecParquetReaderTest extends BaseParquetReaderTest
{
  @Test
  public void testFlat1NoFlattenSpec() throws IOException, InterruptedException
  {
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2", "dim3", "listDim"))),
        ImmutableList.of("metric1", "metric2")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(false, ImmutableList.of());
    InputEntityReader reader = createReader(
        "example/flattening/test_flat_1.parquet",
        schema,
        flattenSpec
    );

    List<InputRow> rows = readAllRows(reader);
    Assert.assertEquals(FlattenSpecParquetInputTest.TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    Assert.assertEquals("d2v1", rows.get(0).getDimension("dim2").get(0));
    Assert.assertEquals("1", rows.get(0).getDimension("dim3").get(0));
    Assert.assertEquals("listDim1v1", rows.get(0).getDimension("listDim").get(0));
    Assert.assertEquals("listDim1v2", rows.get(0).getDimension("listDim").get(1));
    Assert.assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }

  @Test
  public void testFlat1Autodiscover() throws IOException, InterruptedException
  {
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of())),
        ImmutableList.of("metric1", "metric2")
    );
    InputEntityReader reader = createReader(
        "example/flattening/test_flat_1.parquet",
        schema,
        JSONPathSpec.DEFAULT
    );

    List<InputRow> rows = readAllRows(reader);
    Assert.assertEquals(FlattenSpecParquetInputTest.TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    Assert.assertEquals("d2v1", rows.get(0).getDimension("dim2").get(0));
    Assert.assertEquals("1", rows.get(0).getDimension("dim3").get(0));
    Assert.assertEquals("listDim1v1", rows.get(0).getDimension("listDim").get(0));
    Assert.assertEquals("listDim1v2", rows.get(0).getDimension("listDim").get(1));
    Assert.assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }

  @Test
  public void testFlat1Flatten() throws IOException, InterruptedException
  {
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2", "dim3", "list"))),
        ImmutableList.of("metric1", "metric2")
    );
    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "timestamp", null),
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "dim1", null),
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "dim2", null),
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "dim3", null),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "list", "$.listDim")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(false, flattenExpr);
    InputEntityReader reader = createReader(
        "example/flattening/test_flat_1.parquet",
        schema,
        flattenSpec
    );

    List<InputRow> rows = readAllRows(reader);
    Assert.assertEquals(FlattenSpecParquetInputTest.TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    Assert.assertEquals("d2v1", rows.get(0).getDimension("dim2").get(0));
    Assert.assertEquals("1", rows.get(0).getDimension("dim3").get(0));
    Assert.assertEquals("listDim1v1", rows.get(0).getDimension("list").get(0));
    Assert.assertEquals("listDim1v2", rows.get(0).getDimension("list").get(1));
    Assert.assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }

  @Test
  public void testFlat1FlattenSelectListItem() throws IOException, InterruptedException
  {
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2", "listExtracted"))),
        ImmutableList.of("metric1", "metric2")
    );
    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "timestamp", null),
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "dim1", null),
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "dim2", null),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "listExtracted", "$.listDim[1]")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(false, flattenExpr);
    InputEntityReader reader = createReader(
        "example/flattening/test_flat_1.parquet",
        schema,
        flattenSpec
    );

    List<InputRow> rows = readAllRows(reader);
    Assert.assertEquals(FlattenSpecParquetInputTest.TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    Assert.assertEquals("d2v1", rows.get(0).getDimension("dim2").get(0));
    Assert.assertEquals("listDim1v2", rows.get(0).getDimension("listExtracted").get(0));
    Assert.assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }


  @Test
  public void testNested1NoFlattenSpec() throws IOException
  {
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1"))),
        ImmutableList.of("metric1")
    );
    InputEntityReader reader = createReader(
        "example/flattening/test_nested_1.parquet",
        schema,
        new JSONPathSpec(false, ImmutableList.of())
    );

    List<InputRow> rows = readAllRows(reader);
    Assert.assertEquals(FlattenSpecParquetInputTest.TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    List<String> dims = rows.get(0).getDimensions();
    Assert.assertEquals(1, dims.size());
    Assert.assertFalse(dims.contains("dim2"));
    Assert.assertFalse(dims.contains("dim3"));
    Assert.assertFalse(dims.contains("listDim"));
    Assert.assertFalse(dims.contains("nestedData"));
    Assert.assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }

  @Test
  public void testNested1Autodiscover() throws IOException
  {
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of())),
        ImmutableList.of("metric1", "metric2")
    );
    InputEntityReader reader = createReader(
        "example/flattening/test_nested_1.parquet",
        schema,
        JSONPathSpec.DEFAULT
    );

    List<InputRow> rows = readAllRows(reader);
    Assert.assertEquals(FlattenSpecParquetInputTest.TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    List<String> dims = rows.get(0).getDimensions();
    Assert.assertFalse(dims.contains("dim2"));
    Assert.assertFalse(dims.contains("dim3"));
    Assert.assertFalse(dims.contains("listDim"));
    Assert.assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }

  @Test
  public void testNested1Flatten() throws IOException
  {
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of())),
        ImmutableList.of("metric1", "metric2")
    );
    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "timestamp", null),
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "dim1", null),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "dim2", "$.nestedData.dim2"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "dim3", "$.nestedData.dim3"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "metric2", "$.nestedData.metric2"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "listDim", "$.nestedData.listDim[*]")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(true, flattenExpr);
    InputEntityReader reader = createReader(
        "example/flattening/test_nested_1.parquet",
        schema,
        flattenSpec
    );

    List<InputRow> rows = readAllRows(reader);
    Assert.assertEquals(FlattenSpecParquetInputTest.TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    Assert.assertEquals("d2v1", rows.get(0).getDimension("dim2").get(0));
    Assert.assertEquals("1", rows.get(0).getDimension("dim3").get(0));
    Assert.assertEquals("listDim1v1", rows.get(0).getDimension("listDim").get(0));
    Assert.assertEquals("listDim1v2", rows.get(0).getDimension("listDim").get(1));
    Assert.assertEquals(1, rows.get(0).getMetric("metric1").longValue());
    Assert.assertEquals(2, rows.get(0).getMetric("metric2").longValue());
  }

  @Test
  public void testNested1FlattenSelectListItem() throws IOException
  {
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of())),
        Collections.emptyList()
    );
    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "timestamp", null),
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "dim1", null),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "dim2", "$.nestedData.dim2"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "dim3", "$.nestedData.dim3"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "listextracted", "$.nestedData.listDim[1]")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(true, flattenExpr);
    InputEntityReader reader = createReader(
        "example/flattening/test_nested_1.parquet",
        schema,
        flattenSpec
    );

    List<InputRow> rows = readAllRows(reader);

    Assert.assertEquals(FlattenSpecParquetInputTest.TS1, rows.get(0).getTimestamp().toString());
    Assert.assertEquals("d1v1", rows.get(0).getDimension("dim1").get(0));
    Assert.assertEquals("d2v1", rows.get(0).getDimension("dim2").get(0));
    Assert.assertEquals("1", rows.get(0).getDimension("dim3").get(0));
    Assert.assertEquals("listDim1v2", rows.get(0).getDimension("listextracted").get(0));
    Assert.assertEquals(1, rows.get(0).getMetric("metric1").longValue());
  }
}
