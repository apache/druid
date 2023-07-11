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

package org.apache.druid.segment.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class ReaderUtilsTest extends InitializedNullHandlingTest
{

  private final Set<String> fullInputSchema = ImmutableSet.of("A", "B", "C", "D", "E", "F", "G", "H", "I");

  @Test
  public void testGetColumnsRequiredForIngestionWithoutMetricsWithoutTransformAndWithoutFlatten()
  {
    TimestampSpec timestampSpec = new TimestampSpec("A", "iso", null);
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("B"),
            new LongDimensionSchema("C"),
            new FloatDimensionSchema("D")
        )
    );

    Set<String> actual = ReaderUtils.getColumnsRequiredForIngestion(fullInputSchema, timestampSpec, dimensionsSpec, TransformSpec.NONE, new AggregatorFactory[]{}, null);
    Assert.assertEquals(ImmutableSet.of("A", "B", "C", "D"), actual);
  }

  @Test
  public void testGetColumnsRequiredForIngestionWithoutTransformAndWithoutFlatten()
  {
    TimestampSpec timestampSpec = new TimestampSpec("A", "iso", null);
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("B"),
            new LongDimensionSchema("C"),
            new FloatDimensionSchema("D")
        )
    );
    AggregatorFactory[] aggregators = new AggregatorFactory[]{
        new CountAggregatorFactory("custom_count"),
        new LongSumAggregatorFactory("custom_long_sum", "E"),
        new FloatMinAggregatorFactory("custom_float_min", "F")
    };

    Set<String> actual = ReaderUtils.getColumnsRequiredForIngestion(fullInputSchema, timestampSpec, dimensionsSpec, TransformSpec.NONE, aggregators, null);
    Assert.assertEquals(ImmutableSet.of("A", "B", "C", "D", "E", "F"), actual);
  }

  @Test
  public void testGetColumnsRequiredForIngestionWithTransformAndWithoutFlatten()
  {
    TimestampSpec timestampSpec = new TimestampSpec("A", "iso", null);
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("B"),
            new StringDimensionSchema("C*"),
            new LongDimensionSchema("D*"),
            new FloatDimensionSchema("E*"),
            new LongDimensionSchema("G")
        )
    );

    TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            // Function and rename
            new ExpressionTransform("C*", "json_value(C, '$.dim2')", TestExprMacroTable.INSTANCE),
            // Rename
            new ExpressionTransform("D*", "D", TestExprMacroTable.INSTANCE),
            // Function with multiple input columns
            new ExpressionTransform("E*", "concat(E, F)", TestExprMacroTable.INSTANCE),
            // Function with same name
            new ExpressionTransform("G", "CAST(G, LONG)", TestExprMacroTable.INSTANCE)
        )
    );

    Set<String> actual = ReaderUtils.getColumnsRequiredForIngestion(fullInputSchema, timestampSpec, dimensionsSpec, transformSpec, new AggregatorFactory[]{}, null);
    Assert.assertEquals(ImmutableSet.of("A", "B", "C", "D", "E", "F", "G"), actual);
  }

  @Test
  public void testGetColumnsRequiredForIngestionWithFlattenAndUseFieldDiscoveryFalse()
  {
    TimestampSpec timestampSpec = new TimestampSpec("A", "iso", null);
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("B"),
            new StringDimensionSchema("C*"),
            new StringDimensionSchema("D*"),
            // This will not be ingested as it is not in the flattenSpec and useFieldDiscovery is false
            new StringDimensionSchema("E*"),
            // This will not be ingested as it is not in the flattenSpec and useFieldDiscovery is false
            new StringDimensionSchema("F*"),
            // This will not be ingested as it is not in the flattenSpec and useFieldDiscovery is false
            new StringDimensionSchema("H")
        )
    );

    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "B", "B"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "C*", "$.C"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "D*", "$.D.M[*].T")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(false, flattenExpr);

    TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            // This will not be ingested as it is not in the flattenSpec and useFieldDiscovery is false
            new ExpressionTransform("E*", "E", TestExprMacroTable.INSTANCE),
            // This will not be ingested as it is not in the flattenSpec and useFieldDiscovery is false
            new ExpressionTransform("F*", "concat(F, G)", TestExprMacroTable.INSTANCE)
        )
    );

    Set<String> actual = ReaderUtils.getColumnsRequiredForIngestion(fullInputSchema, timestampSpec, dimensionsSpec, transformSpec, new AggregatorFactory[]{}, flattenSpec);
    Assert.assertEquals(ImmutableSet.of("A", "B", "C", "D"), actual);
  }

  @Test
  public void testGetColumnsRequiredForIngestionWithFlattenAndUseFieldDiscoveryTrue()
  {
    TimestampSpec timestampSpec = new TimestampSpec("A", "iso", null);
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("B"),
            new StringDimensionSchema("C*"),
            new StringDimensionSchema("D*"),
            new StringDimensionSchema("E*"),
            new StringDimensionSchema("F*"),
            new StringDimensionSchema("H")
        )
    );

    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "B", "B"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "C*", "$.C"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "D*", "$.D.M[*].T")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(true, flattenExpr);

    TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExpressionTransform("E*", "E", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("F*", "concat(F, G)", TestExprMacroTable.INSTANCE)
        )
    );

    Set<String> actual = ReaderUtils.getColumnsRequiredForIngestion(fullInputSchema, timestampSpec, dimensionsSpec, transformSpec, new AggregatorFactory[]{}, flattenSpec);
    Assert.assertEquals(ImmutableSet.of("A", "B", "C", "D", "E", "F", "G", "H"), actual);
  }

  @Test
  public void testGetColumnsRequiredForIngestionWithFlattenDeepScan()
  {
    TimestampSpec timestampSpec = new TimestampSpec("A", "iso", null);
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("B"),
            new StringDimensionSchema("C*"),
            new StringDimensionSchema("D*"),
            // This will not be ingested as it is not in the flattenSpec and useFieldDiscovery is false
            new StringDimensionSchema("E*"),
            // This will not be ingested as it is not in the flattenSpec and useFieldDiscovery is false
            new StringDimensionSchema("F*"),
            // This will not be ingested as it is not in the flattenSpec and useFieldDiscovery is false
            new StringDimensionSchema("H")
        )
    );

    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "B", "B"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "C*", "$.C"),
        // This is doing a deep scan (need to read all columns)
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "D*", "$..D")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(false, flattenExpr);

    Set<String> actual = ReaderUtils.getColumnsRequiredForIngestion(fullInputSchema, timestampSpec, dimensionsSpec, TransformSpec.NONE, new AggregatorFactory[]{}, flattenSpec);
    Assert.assertEquals(fullInputSchema, actual);
  }

  @Test
  public void testGetColumnsRequiredForIngestionWithFlattenWildcard()
  {
    TimestampSpec timestampSpec = new TimestampSpec("A", "iso", null);
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("B"),
            new StringDimensionSchema("C*"),
            new StringDimensionSchema("D*"),
            // This will not be ingested as it is not in the flattenSpec and useFieldDiscovery is false
            new StringDimensionSchema("E*"),
            // This will not be ingested as it is not in the flattenSpec and useFieldDiscovery is false
            new StringDimensionSchema("F*"),
            // This will not be ingested as it is not in the flattenSpec and useFieldDiscovery is false
            new StringDimensionSchema("H")
        )
    );

    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "B", "B"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "C*", "$.C"),
        // This is doing a wildcard (need to read all columns)
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "D*", "$.*")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(false, flattenExpr);

    Set<String> actual = ReaderUtils.getColumnsRequiredForIngestion(fullInputSchema, timestampSpec, dimensionsSpec, TransformSpec.NONE, new AggregatorFactory[]{}, flattenSpec);
    Assert.assertEquals(fullInputSchema, actual);
  }

  @Test
  public void testGetColumnsRequiredForIngestionWithUnmatchedGroup()
  {
    TimestampSpec timestampSpec = new TimestampSpec("A", "iso", null);
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("B"),
            new StringDimensionSchema("C*"),
            new StringDimensionSchema("D*"),
            // This will not be ingested as it is not in the flattenSpec and useFieldDiscovery is false
            new StringDimensionSchema("E*"),
            // This will not be ingested as it is not in the flattenSpec and useFieldDiscovery is false
            new StringDimensionSchema("F*"),
            // This will not be ingested as it is not in the flattenSpec and useFieldDiscovery is false
            new StringDimensionSchema("H")
        )
    );

    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "B", "B"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "C*", "$.C"),
        // This is doing a wildcard (need to read all columns)
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "D*", "$.[2:4].D")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(false, flattenExpr);

    Set<String> actual = ReaderUtils.getColumnsRequiredForIngestion(fullInputSchema, timestampSpec, dimensionsSpec, TransformSpec.NONE, new AggregatorFactory[]{}, flattenSpec);
    Assert.assertEquals(fullInputSchema, actual);
  }

  @Test
  public void testGetColumnsRequiredForIngestionWithUnsupportedJsonPathFieldType()
  {
    TimestampSpec timestampSpec = new TimestampSpec("A", "iso", null);
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("B"),
            new StringDimensionSchema("C*"),
            new StringDimensionSchema("D*"),
            // This will not be ingested as it is not in the flattenSpec and useFieldDiscovery is false
            new StringDimensionSchema("E*"),
            // This will not be ingested as it is not in the flattenSpec and useFieldDiscovery is false
            new StringDimensionSchema("F*"),
            // This will not be ingested as it is not in the flattenSpec and useFieldDiscovery is false
            new StringDimensionSchema("H")
        )
    );

    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.ROOT, "B", "B"),
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "C*", "$.C"),
        // This is unsupported. Hence, return all columns
        new JSONPathFieldSpec(JSONPathFieldType.JQ, "foobar", ".foo.bar")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(false, flattenExpr);

    Set<String> actual = ReaderUtils.getColumnsRequiredForIngestion(fullInputSchema, timestampSpec, dimensionsSpec, TransformSpec.NONE, new AggregatorFactory[]{}, flattenSpec);
    Assert.assertEquals(fullInputSchema, actual);
  }

  @Test
  public void testGetColumnsRequiredForIngestionWithFlattenTimestamp()
  {
    TimestampSpec timestampSpec = new TimestampSpec("CFlat", "iso", null);
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        ImmutableList.of(
            new StringDimensionSchema("B")
        )
    );
    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "CFlat", "$.C.time")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(true, flattenExpr);

    Set<String> actual = ReaderUtils.getColumnsRequiredForIngestion(fullInputSchema, timestampSpec, dimensionsSpec, TransformSpec.NONE, new AggregatorFactory[]{}, flattenSpec);
    Assert.assertEquals(ImmutableSet.of("B", "C"), actual);
  }

  @Test
  public void testGetColumnsRequiredForSchemalessIngestionWithoutFlattenSpec()
  {
    TimestampSpec timestampSpec = new TimestampSpec("A", "iso", null);
    DimensionsSpec dimensionsSpec = DimensionsSpec.EMPTY;

    Set<String> actual = ReaderUtils.getColumnsRequiredForIngestion(fullInputSchema, timestampSpec, dimensionsSpec, TransformSpec.NONE, new AggregatorFactory[]{}, null);
    Assert.assertEquals(fullInputSchema, actual);
  }

  @Test
  public void testGetColumnsRequiredForSchemalessIngestionWithFlattenSpecAndUseFieldDiscovery()
  {
    TimestampSpec timestampSpec = new TimestampSpec("A", "iso", null);
    DimensionsSpec dimensionsSpec = DimensionsSpec.EMPTY;
    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "CFlat", "$.C.time")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(true, flattenExpr);
    Set<String> actual = ReaderUtils.getColumnsRequiredForIngestion(fullInputSchema, timestampSpec, dimensionsSpec, TransformSpec.NONE, new AggregatorFactory[]{}, flattenSpec);
    Assert.assertEquals(fullInputSchema, actual);
  }

  @Test
  public void testGetColumnsRequiredForSchemalessIngestionWithFlattenSpecAndNotUseFieldDiscovery()
  {
    TimestampSpec timestampSpec = new TimestampSpec("A", "iso", null);
    DimensionsSpec dimensionsSpec = DimensionsSpec.EMPTY;
    List<JSONPathFieldSpec> flattenExpr = ImmutableList.of(
        new JSONPathFieldSpec(JSONPathFieldType.PATH, "CFlat", "$.C.time")
    );
    JSONPathSpec flattenSpec = new JSONPathSpec(false, flattenExpr);
    Set<String> actual = ReaderUtils.getColumnsRequiredForIngestion(fullInputSchema, timestampSpec, dimensionsSpec, TransformSpec.NONE, new AggregatorFactory[]{}, flattenSpec);
    Assert.assertEquals(ImmutableSet.of("A", "C"), actual);
  }
}
