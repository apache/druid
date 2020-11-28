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

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class ExpressionVectorSelectorsTest
{
  private static List<String> EXPRESSIONS = ImmutableList.of(
      "long1 * long2",
      "long1 * nonexistent",
      "double1 * double3",
      "float1 + float3",
      "(long1 - long4) / double3",
      "long5 * float3 * long1 * long4 * double1",
      "long5 * double3 * long1 * long4 * double1",
      "max(double3, double5)",
      "max(nonexistent, double5)",
      "min(double4, double1)",
      "cos(float3)",
      "sin(long4)",
      "parse_long(string1)",
      "parse_long(nonexistent)",
      "parse_long(string1) * double3",
      "parse_long(string5) * parse_long(string1)",
      "parse_long(string5) * parse_long(string1) * double3",
      "'string constant'",
      "1",
      "192412.24124",
      "null",
      "long2",
      "float2",
      "double2",
      "string3"
  );

  private static final int ROWS_PER_SEGMENT = 100_000;

  private static QueryableIndex INDEX;
  private static Closer CLOSER;

  @BeforeClass
  public static void setupClass()
  {
    CLOSER = Closer.create();

    final GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("expression-testbench");

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();

    final SegmentGenerator segmentGenerator = CLOSER.register(new SegmentGenerator());
    INDEX = CLOSER.register(
        segmentGenerator.generate(dataSegment, schemaInfo, Granularities.HOUR, ROWS_PER_SEGMENT)
    );
  }

  @AfterClass
  public static void teardownClass() throws IOException
  {
    CLOSER.close();
  }

  @Parameterized.Parameters(name = "expression = {0}")
  public static Iterable<?> constructorFeeder()
  {
    return EXPRESSIONS.stream().map(x -> new Object[]{x}).collect(Collectors.toList());
  }

  @Nullable
  private ExprType outputType;
  private String expression;

  public ExpressionVectorSelectorsTest(String expression)
  {
    this.expression = expression;
  }

  @Before
  public void setup()
  {
    Expr parsed = Parser.parse(expression, ExprMacroTable.nil());
    outputType = parsed.getOutputType(
        new ColumnInspector()
        {
          @Nullable
          @Override
          public ColumnCapabilities getColumnCapabilities(String column)
          {
            return QueryableIndexStorageAdapter.getColumnCapabilities(INDEX, column);
          }
        }
    );
  }

  @Test
  public void sanityTestVectorizedExpressionSelector()
  {
    sanityTestVectorizedExpressionSelectors(expression, outputType, INDEX, CLOSER, ROWS_PER_SEGMENT);
  }

  public static void sanityTestVectorizedExpressionSelectors(
      String expression,
      @Nullable ExprType outputType,
      QueryableIndex index,
      Closer closer,
      int rowsPerSegment
  )
  {
    final List<Object> results = new ArrayList<>(rowsPerSegment);
    final VirtualColumns virtualColumns = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn(
                "v",
                expression,
                ExprType.toValueType(outputType),
                TestExprMacroTable.INSTANCE
            )
        )
    );
    final QueryableIndexStorageAdapter storageAdapter = new QueryableIndexStorageAdapter(index);
    VectorCursor cursor = storageAdapter.makeVectorCursor(
        null,
        index.getDataInterval(),
        virtualColumns,
        false,
        512,
        null
    );

    ColumnCapabilities capabilities = virtualColumns.getColumnCapabilities(storageAdapter, "v");

    int rowCount = 0;
    if (capabilities.isDictionaryEncoded().isTrue()) {
      SingleValueDimensionVectorSelector selector = cursor.getColumnSelectorFactory().makeSingleValueDimensionSelector(
          DefaultDimensionSpec.of("v")
      );
      while (!cursor.isDone()) {
        int[] row = selector.getRowVector();
        for (int i = 0; i < selector.getCurrentVectorSize(); i++, rowCount++) {
          results.add(selector.lookupName(row[i]));
        }
        cursor.advance();
      }
    } else {
      VectorValueSelector selector = null;
      VectorObjectSelector objectSelector = null;
      if (outputType.isNumeric()) {
        selector = cursor.getColumnSelectorFactory().makeValueSelector("v");
      } else {
        objectSelector = cursor.getColumnSelectorFactory().makeObjectSelector("v");
      }
      while (!cursor.isDone()) {
        boolean[] nulls;
        switch (outputType) {
          case LONG:
            nulls = selector.getNullVector();
            long[] longs = selector.getLongVector();
            for (int i = 0; i < selector.getCurrentVectorSize(); i++, rowCount++) {
              results.add(nulls != null && nulls[i] ? null : longs[i]);
            }
            break;
          case DOUBLE:
            // special case to test floats just to get coverage on getFloatVector
            if ("float2".equals(expression)) {
              nulls = selector.getNullVector();
              float[] floats = selector.getFloatVector();
              for (int i = 0; i < selector.getCurrentVectorSize(); i++, rowCount++) {
                results.add(nulls != null && nulls[i] ? null : (double) floats[i]);
              }
            } else {
              nulls = selector.getNullVector();
              double[] doubles = selector.getDoubleVector();
              for (int i = 0; i < selector.getCurrentVectorSize(); i++, rowCount++) {
                results.add(nulls != null && nulls[i] ? null : doubles[i]);
              }
            }
            break;
          case STRING:
            Object[] objects = objectSelector.getObjectVector();
            for (int i = 0; i < objectSelector.getCurrentVectorSize(); i++, rowCount++) {
              results.add(objects[i]);
            }
            break;
        }

        cursor.advance();
      }
    }
    closer.register(cursor);

    Sequence<Cursor> cursors = new QueryableIndexStorageAdapter(index).makeCursors(
        null,
        index.getDataInterval(),
        virtualColumns,
        Granularities.ALL,
        false,
        null
    );

    int rowCountCursor = cursors
        .map(nonVectorized -> {
          final ColumnValueSelector nonSelector = nonVectorized.getColumnSelectorFactory()
                                                               .makeColumnValueSelector("v");
          int rows = 0;
          while (!nonVectorized.isDone()) {
            Assert.assertEquals(
                StringUtils.format("Failed at row %s", rows),
                nonSelector.getObject(),
                results.get(rows)
            );
            rows++;
            nonVectorized.advance();
          }
          return rows;
        }).accumulate(0, (acc, in) -> acc + in);

    Assert.assertTrue(rowCountCursor > 0);
    Assert.assertEquals(rowCountCursor, rowCount);
  }
}
