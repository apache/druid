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
import org.apache.druid.segment.CloserExtension;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 */
@ParameterizedClass
@MethodSource("constructorFeeder")
public class IncrementalIndexTest extends InitializedNullHandlingTest
{
  public final IncrementalIndexCreator indexCreator;

  private final String mode;

  @RegisterExtension
  public final CloserExtension closer = new CloserExtension(false);

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
            AutoTypeColumnSchema.of("bool_auto"),
            new AutoTypeColumnSchema("array_string", ColumnType.STRING_ARRAY, null),
            new AutoTypeColumnSchema("array_double", ColumnType.DOUBLE_ARRAY, null),
            new AutoTypeColumnSchema("array_long", ColumnType.LONG_ARRAY, null),
            AutoTypeColumnSchema.of("nested")
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

  public static Collection<?> constructorFeeder()
  {
    return IncrementalIndexCreator.indexTypeCartesianProduct(
        ImmutableList.of("rollup", "plain")
    );
  }

  @Test
  public void testDuplicateDimensions()
  {
    Assertions.assertThrows(ISE.class, () -> {
      final IncrementalIndex index = indexCreator.createIndex();
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
    });
  }

  @Test
  public void testDuplicateDimensionsFirstOccurrence()
  {
    Assertions.assertThrows(ISE.class, () -> {
      final IncrementalIndex index = indexCreator.createIndex();
      index.add(
          new MapBasedInputRow(
              System.currentTimeMillis() - 1,
              Lists.newArrayList("billy", "joe", "joe"),
              ImmutableMap.of("billy", "A", "joe", "B")
          )
      );
    });
  }

  @Test
  public void controlTest()
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
  public void testUnparseableNumerics()
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
    Assertions.assertEquals(UnparseableColumnsParseException.class, result.getParseException().getClass());
    Assertions.assertEquals(
        "{string=A, float=19.0, long=asdj, double=21.0}",
        result.getParseException().getInput()
    );
    Assertions.assertEquals(
        "Found unparseable columns in row: [{string=A, float=19.0, long=asdj, double=21.0}], exceptions: [Could not convert value [asdj] to long for dimension [long].]",
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
    Assertions.assertEquals(UnparseableColumnsParseException.class, result.getParseException().getClass());
    Assertions.assertEquals(
        "{string=A, float=aaa, long=20, double=21.0}",
        result.getParseException().getInput()
    );
    Assertions.assertEquals(
        "Found unparseable columns in row: [{string=A, float=aaa, long=20, double=21.0}], exceptions: [Could not convert value [aaa] to float for dimension [float].]",
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
    Assertions.assertEquals(UnparseableColumnsParseException.class, result.getParseException().getClass());
    Assertions.assertEquals(
        "{string=A, float=19.0, long=20, double=}",
        result.getParseException().getInput()
    );
    Assertions.assertEquals(
        "Found unparseable columns in row: [{string=A, float=19.0, long=20, double=}], exceptions: [Could not convert value [] to double for dimension [double].]",
        result.getParseException().getMessage()
    );
  }

  @Test
  public void testMultiValuedNumericDimensions()
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
    Assertions.assertEquals(UnparseableColumnsParseException.class, result.getParseException().getClass());
    Assertions.assertEquals(
        "{string=A, float=19.0, long=[10, 5], double=21.0}",
        result.getParseException().getInput()
    );
    Assertions.assertEquals(
        "Found unparseable columns in row: [{string=A, float=19.0, long=[10, 5], double=21.0}], exceptions: [Could not ingest value [[10, 5]] as long for dimension [long]. A long column cannot have multiple values in the same row.]",
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
    Assertions.assertEquals(UnparseableColumnsParseException.class, result.getParseException().getClass());
    Assertions.assertEquals(
        "{string=A, float=[10.0, 5.0], long=20, double=21.0}",
        result.getParseException().getInput()
    );
    Assertions.assertEquals(
        "Found unparseable columns in row: [{string=A, float=[10.0, 5.0], long=20, double=21.0}], exceptions: [Could not ingest value [[10.0, 5.0]] as float for dimension [float]. A float column cannot have multiple values in the same row.]",
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
    Assertions.assertEquals(UnparseableColumnsParseException.class, result.getParseException().getClass());
    Assertions.assertEquals(
        "{string=A, float=19.0, long=20, double=[10.0, 5.0]}",
        result.getParseException().getInput()
    );
    Assertions.assertEquals(
        "Found unparseable columns in row: [{string=A, float=19.0, long=20, double=[10.0, 5.0]}], exceptions: [Could not ingest value [[10.0, 5.0]] as double for dimension [double]. A double column cannot have multiple values in the same row.]",
        result.getParseException().getMessage()
    );
  }

  @Test
  public void sameRow()
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

    Assertions.assertEquals("rollup".equals(mode) ? 1 : 3, index.numRows());
  }

  @Test
  public void testTypeHandling()
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
    Assertions.assertNull(result.getParseException());
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
    Assertions.assertNull(result.getParseException());

    Assertions.assertEquals(ColumnType.STRING, index.getColumnCapabilities("string").toColumnType());
    Assertions.assertEquals(ColumnType.FLOAT, index.getColumnCapabilities("float").toColumnType());
    Assertions.assertEquals(ColumnType.LONG, index.getColumnCapabilities("long").toColumnType());
    Assertions.assertEquals(ColumnType.DOUBLE, index.getColumnCapabilities("double").toColumnType());
    Assertions.assertEquals(ColumnType.STRING, index.getColumnCapabilities("bool_string").toColumnType());
    Assertions.assertEquals(ColumnType.LONG, index.getColumnCapabilities("bool_long").toColumnType());
    Assertions.assertEquals(ColumnType.LONG, index.getColumnCapabilities("bool_auto").toColumnType());
    Assertions.assertEquals(ColumnType.STRING_ARRAY, index.getColumnCapabilities("array_string").toColumnType());
    Assertions.assertEquals(ColumnType.LONG_ARRAY, index.getColumnCapabilities("array_long").toColumnType());
    Assertions.assertEquals(ColumnType.DOUBLE_ARRAY, index.getColumnCapabilities("array_double").toColumnType());
    Assertions.assertEquals(ColumnType.NESTED_DATA, index.getColumnCapabilities("nested").toColumnType());


    Iterator<Row> rowIterator = index.iterator();
    Row row = rowIterator.next();
    Assertions.assertEquals("a", row.getRaw("string"));
    Assertions.assertEquals(1.0f, row.getRaw("float"));
    Assertions.assertEquals(1L, row.getRaw("long"));
    Assertions.assertEquals(1.0, row.getRaw("double"));
    Assertions.assertEquals("true", row.getRaw("bool_string"));
    Assertions.assertEquals(1L, row.getRaw("bool_long"));
    Assertions.assertEquals(StructuredData.wrap(true), row.getRaw("bool_auto"));
    Assertions.assertEquals(StructuredData.wrap(new Object[]{"a", "b", "c"}), row.getRaw("array_string"));
    Assertions.assertEquals(StructuredData.wrap(new Object[]{1L, 2L, 3L}), row.getRaw("array_long"));
    Assertions.assertEquals(StructuredData.wrap(new Object[]{1.1, 2.2, 3.3}), row.getRaw("array_double"));
    Assertions.assertEquals(StructuredData.wrap(ImmutableMap.of("x", 1, "y", ImmutableList.of("a", "b"))), row.getRaw("nested"));

    row = rowIterator.next();
    Assertions.assertEquals("b", row.getRaw("string"));
    Assertions.assertEquals(2.0f, row.getRaw("float"));
    Assertions.assertEquals(2L, row.getRaw("long"));
    Assertions.assertEquals(2.0, row.getRaw("double"));
    Assertions.assertEquals("false", row.getRaw("bool_string"));
    Assertions.assertEquals(0L, row.getRaw("bool_long"));
    Assertions.assertEquals(StructuredData.wrap(false), row.getRaw("bool_auto"));
    Assertions.assertEquals(StructuredData.wrap(new Object[]{"d", "e", "f"}), row.getRaw("array_string"));
    Assertions.assertEquals(StructuredData.wrap(new Object[]{4L, 5L, 6L}), row.getRaw("array_long"));
    Assertions.assertEquals(StructuredData.wrap(new Object[]{4.4, 5.5, 6.6}), row.getRaw("array_double"));
    Assertions.assertEquals(StructuredData.wrap(ImmutableMap.of("x", 2, "y", ImmutableList.of("c", "d"))), row.getRaw("nested"));
  }
}
