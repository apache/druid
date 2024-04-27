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

package org.apache.druid.segment.incremental;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.UnparseableColumnsParseException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.CloserRule;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class IncrementalIndexTest extends InitializedNullHandlingTest
{
  public final IncrementalIndexCreator indexCreator;

  private final String mode;

  @Rule
  public final CloserRule closer = new CloserRule(false);

  public IncrementalIndexTest(
      String indexType,
      String mode
  ) throws JsonProcessingException
  {
    this.mode = mode;
    DimensionsSpec dimensions = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("string"),
            new FloatDimensionSchema("float"),
            new LongDimensionSchema("long"),
            new DoubleDimensionSchema("double"),
            new StringDimensionSchema("bool_string"),
            new LongDimensionSchema("bool_long"),
            new AutoTypeColumnSchema("bool_auto", null),
            new AutoTypeColumnSchema("array_string", ColumnType.STRING_ARRAY),
            new AutoTypeColumnSchema("array_double", ColumnType.DOUBLE_ARRAY),
            new AutoTypeColumnSchema("array_long", ColumnType.LONG_ARRAY),
            new AutoTypeColumnSchema("nested", null)
        )
    );
    AggregatorFactory[] metrics = {
        new FilteredAggregatorFactory(
            new CountAggregatorFactory("cnt"),
            new SelectorDimFilter("billy", "A", null)
        )
    };
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withQueryGranularity(Granularities.MINUTE)
        .withDimensionsSpec(dimensions)
        .withMetrics(metrics)
        .withRollup("rollup".equals(mode))
        .build();
    indexCreator = closer.closeLater(new IncrementalIndexCreator(indexType, (builder, args) -> builder
        .setIndexSchema(schema)
        .setMaxRowCount(1_000_000)
        .build())
    );
  }

  @Parameterized.Parameters(name = "{index}: {0}, {1}")
  public static Collection<?> constructorFeeder()
  {
    return IncrementalIndexCreator.indexTypeCartesianProduct(
        ImmutableList.of("rollup", "plain")
    );
  }

  @Test(expected = ISE.class)
  public void testDuplicateDimensions() throws IndexSizeExceededException
  {
    IncrementalIndex index = indexCreator.createIndex();
    index.add(
        new MapBasedInputRow(
            System.currentTimeMillis() - 1,
            Lists.newArrayList("billy", "joe"),
            ImmutableMap.of("billy", "A", "joe", "B")
        )
    );
    index.add(
        new MapBasedInputRow(
            System.currentTimeMillis() - 1,
            Lists.newArrayList("billy", "joe", "joe"),
            ImmutableMap.of("billy", "A", "joe", "B")
        )
    );
  }

  @Test(expected = ISE.class)
  public void testDuplicateDimensionsFirstOccurrence() throws IndexSizeExceededException
  {
    IncrementalIndex index = indexCreator.createIndex();
    index.add(
        new MapBasedInputRow(
            System.currentTimeMillis() - 1,
            Lists.newArrayList("billy", "joe", "joe"),
            ImmutableMap.of("billy", "A", "joe", "B")
        )
    );
  }

  @Test
  public void controlTest() throws IndexSizeExceededException
  {
    IncrementalIndex index = indexCreator.createIndex();
    index.add(
        new MapBasedInputRow(
            System.currentTimeMillis() - 1,
            Lists.newArrayList("billy", "joe"),
            ImmutableMap.of("billy", "A", "joe", "B")
        )
    );
    index.add(
        new MapBasedInputRow(
            System.currentTimeMillis() - 1,
            Lists.newArrayList("billy", "joe"),
            ImmutableMap.of("billy", "C", "joe", "B")
        )
    );
    index.add(
        new MapBasedInputRow(
            System.currentTimeMillis() - 1,
            Lists.newArrayList("billy", "joe"),
            ImmutableMap.of("billy", "A", "joe", "B")
        )
    );
  }

  @Test
  public void testUnparseableNumerics() throws IndexSizeExceededException
  {
    IncrementalIndex index = indexCreator.createIndex();

    IncrementalIndexAddResult result;
    result = index.add(
        new MapBasedInputRow(
            0,
            Lists.newArrayList("string", "float", "long", "double"),
            ImmutableMap.of(
                "string", "A",
                "float", "19.0",
                "long", "asdj",
                "double", 21.0d
            )
        )
    );
    Assert.assertEquals(UnparseableColumnsParseException.class, result.getParseException().getClass());
    Assert.assertEquals(
        "{string=A, float=19.0, long=asdj, double=21.0}",
        result.getParseException().getInput()
    );
    Assert.assertEquals(
        "Found unparseable columns in row: [{string=A, float=19.0, long=asdj, double=21.0}], exceptions: [could not convert value [asdj] to long]",
        result.getParseException().getMessage()
    );

    result = index.add(
        new MapBasedInputRow(
            0,
            Lists.newArrayList("string", "float", "long", "double"),
            ImmutableMap.of(
                "string", "A",
                "float", "aaa",
                "long", 20,
                "double", 21.0d
            )
        )
    );
    Assert.assertEquals(UnparseableColumnsParseException.class, result.getParseException().getClass());
    Assert.assertEquals(
        "{string=A, float=aaa, long=20, double=21.0}",
        result.getParseException().getInput()
    );
    Assert.assertEquals(
        "Found unparseable columns in row: [{string=A, float=aaa, long=20, double=21.0}], exceptions: [could not convert value [aaa] to float]",
        result.getParseException().getMessage()
    );

    result = index.add(
        new MapBasedInputRow(
            0,
            Lists.newArrayList("string", "float", "long", "double"),
            ImmutableMap.of(
                "string", "A",
                "float", 19.0,
                "long", 20,
                "double", ""
            )
        )
    );
    Assert.assertEquals(UnparseableColumnsParseException.class, result.getParseException().getClass());
    Assert.assertEquals(
        "{string=A, float=19.0, long=20, double=}",
        result.getParseException().getInput()
    );
    Assert.assertEquals(
        "Found unparseable columns in row: [{string=A, float=19.0, long=20, double=}], exceptions: [could not convert value [] to double]",
        result.getParseException().getMessage()
    );
  }

  @Test
  public void testMultiValuedNumericDimensions() throws IndexSizeExceededException
  {
    IncrementalIndex index = indexCreator.createIndex();

    IncrementalIndexAddResult result;
    result = index.add(
        new MapBasedInputRow(
            0,
            Lists.newArrayList("string", "float", "long", "double"),
            ImmutableMap.of(
                "string", "A",
                "float", "19.0",
                "long", Arrays.asList(10L, 5L),
                "double", 21.0d
            )
        )
    );
    Assert.assertEquals(UnparseableColumnsParseException.class, result.getParseException().getClass());
    Assert.assertEquals(
        "{string=A, float=19.0, long=[10, 5], double=21.0}",
        result.getParseException().getInput()
    );
    Assert.assertEquals(
        "Found unparseable columns in row: [{string=A, float=19.0, long=[10, 5], double=21.0}], exceptions: [Could not ingest value [10, 5] as long. A long column cannot have multiple values in the same row.]",
        result.getParseException().getMessage()
    );

    result = index.add(
        new MapBasedInputRow(
            0,
            Lists.newArrayList("string", "float", "long", "double"),
            ImmutableMap.of(
                "string", "A",
                "float", Arrays.asList(10.0f, 5.0f),
                "long", 20,
                "double", 21.0d
            )
        )
    );
    Assert.assertEquals(UnparseableColumnsParseException.class, result.getParseException().getClass());
    Assert.assertEquals(
        "{string=A, float=[10.0, 5.0], long=20, double=21.0}",
        result.getParseException().getInput()
    );
    Assert.assertEquals(
        "Found unparseable columns in row: [{string=A, float=[10.0, 5.0], long=20, double=21.0}], exceptions: [Could not ingest value [10.0, 5.0] as float. A float column cannot have multiple values in the same row.]",
        result.getParseException().getMessage()
    );

    result = index.add(
        new MapBasedInputRow(
            0,
            Lists.newArrayList("string", "float", "long", "double"),
            ImmutableMap.of(
                "string", "A",
                "float", 19.0,
                "long", 20,
                "double", Arrays.asList(10.0D, 5.0D)
            )
        )
    );
    Assert.assertEquals(UnparseableColumnsParseException.class, result.getParseException().getClass());
    Assert.assertEquals(
        "{string=A, float=19.0, long=20, double=[10.0, 5.0]}",
        result.getParseException().getInput()
    );
    Assert.assertEquals(
        "Found unparseable columns in row: [{string=A, float=19.0, long=20, double=[10.0, 5.0]}], exceptions: [Could not ingest value [10.0, 5.0] as double. A double column cannot have multiple values in the same row.]",
        result.getParseException().getMessage()
    );
  }

  @Test
  public void sameRow() throws IndexSizeExceededException
  {
    MapBasedInputRow row = new MapBasedInputRow(
        System.currentTimeMillis() - 1,
        Lists.newArrayList("billy", "joe"),
        ImmutableMap.of("billy", "A", "joe", "B")
    );
    IncrementalIndex index = indexCreator.createIndex();
    index.add(row);
    index.add(row);
    index.add(row);

    Assert.assertEquals("rollup".equals(mode) ? 1 : 3, index.size());
  }

  @Test
  public void testTypeHandling() throws IndexSizeExceededException
  {
    IncrementalIndex index = indexCreator.createIndex();

    final List<String> dims = Arrays.asList(
        "string",
        "float",
        "long",
        "double",
        "bool_string",
        "bool_long",
        "bool_auto",
        "array_string",
        "array_long",
        "array_double",
        "nested"
    );
    IncrementalIndexAddResult result = index.add(
        new MapBasedInputRow(
            0,
            dims,
            ImmutableMap.<String, Object>builder()
                        .put("string", "a")
                        .put("float", 1.0)
                        .put("long", 1)
                        .put("double", 1.0)
                        .put("bool_string", true)
                        .put("bool_long", true)
                        .put("bool_auto", true)
                        .put("array_string", ImmutableList.of("a", "b", "c"))
                        .put("array_long", ImmutableList.of(1, 2, 3))
                        .put("array_double", ImmutableList.of(1.1, 2.2, 3.3))
                        .put("nested", ImmutableMap.of("x", 1, "y", ImmutableList.of("a", "b")))
                        .build()
        )
    );
    Assert.assertNull(result.getParseException());
    result = index.add(
        new MapBasedInputRow(
            60_000, // next minute so non-rollup still orders iterator correctly
            dims,
            ImmutableMap.<String, Object>builder()
                        .put("string", "b")
                        .put("float", 2.0)
                        .put("long", 2)
                        .put("double", 2.0)
                        .put("bool_string", false)
                        .put("bool_long", false)
                        .put("bool_auto", false)
                        .put("array_string", ImmutableList.of("d", "e", "f"))
                        .put("array_long", ImmutableList.of(4, 5, 6))
                        .put("array_double", ImmutableList.of(4.4, 5.5, 6.6))
                        .put("nested", ImmutableMap.of("x", 2, "y", ImmutableList.of("c", "d")))
                        .build()
        )
    );
    Assert.assertNull(result.getParseException());

    Assert.assertEquals(ColumnType.STRING, index.getColumnCapabilities("string").toColumnType());
    Assert.assertEquals(ColumnType.FLOAT, index.getColumnCapabilities("float").toColumnType());
    Assert.assertEquals(ColumnType.LONG, index.getColumnCapabilities("long").toColumnType());
    Assert.assertEquals(ColumnType.DOUBLE, index.getColumnCapabilities("double").toColumnType());
    Assert.assertEquals(ColumnType.STRING, index.getColumnCapabilities("bool_string").toColumnType());
    Assert.assertEquals(ColumnType.LONG, index.getColumnCapabilities("bool_long").toColumnType());
    Assert.assertEquals(ColumnType.LONG, index.getColumnCapabilities("bool_auto").toColumnType());
    Assert.assertEquals(ColumnType.STRING_ARRAY, index.getColumnCapabilities("array_string").toColumnType());
    Assert.assertEquals(ColumnType.LONG_ARRAY, index.getColumnCapabilities("array_long").toColumnType());
    Assert.assertEquals(ColumnType.DOUBLE_ARRAY, index.getColumnCapabilities("array_double").toColumnType());
    Assert.assertEquals(ColumnType.NESTED_DATA, index.getColumnCapabilities("nested").toColumnType());


    Iterator<Row> rowIterator = index.iterator();
    Row row = rowIterator.next();
    Assert.assertEquals("a", row.getRaw("string"));
    Assert.assertEquals(1.0f, row.getRaw("float"));
    Assert.assertEquals(1L, row.getRaw("long"));
    Assert.assertEquals(1.0, row.getRaw("double"));
    Assert.assertEquals("true", row.getRaw("bool_string"));
    Assert.assertEquals(1L, row.getRaw("bool_long"));
    Assert.assertEquals(StructuredData.wrap(true), row.getRaw("bool_auto"));
    Assert.assertEquals(StructuredData.wrap(new Object[]{"a", "b", "c"}), row.getRaw("array_string"));
    Assert.assertEquals(StructuredData.wrap(new Object[]{1L, 2L, 3L}), row.getRaw("array_long"));
    Assert.assertEquals(StructuredData.wrap(new Object[]{1.1, 2.2, 3.3}), row.getRaw("array_double"));
    Assert.assertEquals(StructuredData.wrap(ImmutableMap.of("x", 1, "y", ImmutableList.of("a", "b"))), row.getRaw("nested"));

    row = rowIterator.next();
    Assert.assertEquals("b", row.getRaw("string"));
    Assert.assertEquals(2.0f, row.getRaw("float"));
    Assert.assertEquals(2L, row.getRaw("long"));
    Assert.assertEquals(2.0, row.getRaw("double"));
    Assert.assertEquals("false", row.getRaw("bool_string"));
    Assert.assertEquals(0L, row.getRaw("bool_long"));
    Assert.assertEquals(StructuredData.wrap(false), row.getRaw("bool_auto"));
    Assert.assertEquals(StructuredData.wrap(new Object[]{"d", "e", "f"}), row.getRaw("array_string"));
    Assert.assertEquals(StructuredData.wrap(new Object[]{4L, 5L, 6L}), row.getRaw("array_long"));
    Assert.assertEquals(StructuredData.wrap(new Object[]{4.4, 5.5, 6.6}), row.getRaw("array_double"));
    Assert.assertEquals(StructuredData.wrap(ImmutableMap.of("x", 2, "y", ImmutableList.of("c", "d"))), row.getRaw("nested"));
  }
}
