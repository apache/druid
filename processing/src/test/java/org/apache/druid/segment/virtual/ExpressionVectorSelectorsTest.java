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
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.groupby.DeferExpressionDimensions;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.collection.MemoryPointer;
import org.apache.druid.query.groupby.epinephelinae.vector.GroupByVectorColumnSelector;
import org.apache.druid.segment.ColumnCache;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@ParameterizedClass
@MethodSource("constructorFeeder")
public class ExpressionVectorSelectorsTest extends InitializedNullHandlingTest
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
      "string3",
      "string1 + string3",
      "concat(string1, string2, string3)",
      "concat(string1, 'x')",
      "concat(string1, nonexistent)"
  );

  private static final int ROWS_PER_SEGMENT = 10_000;

  private static QueryableIndex INDEX;
  private static QueryableIndex INDEX_OTHER_ENCODINGS;
  private static Closer CLOSER;

  @BeforeAll
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


    final SegmentGenerator otherGenerator = CLOSER.register(new SegmentGenerator());
    final DataSegment otherSegment = DataSegment.builder()
                                                .dataSource("foo")
                                                .interval(schemaInfo.getDataInterval())
                                                .version("2")
                                                .shardSpec(new LinearShardSpec(0))
                                                .size(0)
                                                .build();
    IndexSpec otherEncodings = IndexSpec.builder()
                                        .withStringDictionaryEncoding(
                                            new StringEncodingStrategy.FrontCoded(16, FrontCodedIndexed.V1)
                                        )
                                        .withLongEncoding(CompressionFactory.LongEncodingStrategy.AUTO)
                                        .build();

    INDEX_OTHER_ENCODINGS = CLOSER.register(
        otherGenerator.generate(otherSegment, schemaInfo, otherEncodings, Granularities.HOUR, ROWS_PER_SEGMENT)
    );
  }

  @AfterAll
  public static void teardownClass() throws IOException
  {
    CLOSER.close();
  }

  public static Stream<Object[]> constructorFeeder()
  {
    List<Object[]> params = new ArrayList<>();
    for (String encoding : new String[]{"default", "front-coded-and-auto-longs"}) {
      for (String expression : EXPRESSIONS) {
        params.add(new Object[]{expression, encoding});
      }
    }
    return params.stream();
  }

  @Parameter(0)
  public String expression;
  @Parameter(1)
  public String encoding;
  private ExpressionType outputType;
  private QueryableIndex queryableIndexToUse;
  private Closer perTestCloser = Closer.create();

  @BeforeEach
  public void setup()
  {
    queryableIndexToUse = "front-coded-and-auto-longs".equals(encoding) ? INDEX_OTHER_ENCODINGS : INDEX;
    Expr parsed = Parser.parse(expression, ExprMacroTable.nil());
    outputType = parsed.getOutputType(new ColumnCache(queryableIndexToUse, VirtualColumns.EMPTY, perTestCloser));
    if (outputType == null) {
      outputType = ExpressionType.STRING;
    }
  }

  @AfterEach
  public void teardown() throws IOException
  {
    perTestCloser.close();
  }


  @Test
  public void sanityTestVectorizedExpressionSelector()
  {
    sanityTestVectorizedExpressionSelectors(expression, outputType, queryableIndexToUse, ROWS_PER_SEGMENT);
  }

  public static void sanityTestVectorizedExpressionSelectors(
      String expression,
      @Nullable ExpressionType outputType,
      QueryableIndex index,
      int rowsPerSegment
  )
  {
    final List<Object> results = new ArrayList<>(rowsPerSegment);
    final VirtualColumns virtualColumns = VirtualColumns.create(
        new ExpressionVirtualColumn(
            "v",
            expression,
            ExpressionType.toColumnType(outputType),
            TestExprMacroTable.INSTANCE
        )
    );
    final QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                     .setVirtualColumns(virtualColumns)
                                                     .build();
    try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec)) {
      final VectorCursor cursor = cursorHolder.asVectorCursor();
      Assertions.assertNotNull(cursor);

      ColumnCapabilities capabilities = virtualColumns.getColumnCapabilitiesWithFallback(cursorFactory, "v");

      int rowCount = 0;
      if (capabilities.isDictionaryEncoded().isTrue()) {
        SingleValueDimensionVectorSelector selector = cursor.getColumnSelectorFactory()
                                                            .makeSingleValueDimensionSelector(
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
        if (Types.isNumeric(outputType)) {
          selector = cursor.getColumnSelectorFactory().makeValueSelector("v");
        } else {
          objectSelector = cursor.getColumnSelectorFactory().makeObjectSelector("v");
        }
        GroupByVectorColumnSelector groupBySelector =
            cursor.getColumnSelectorFactory().makeGroupByVectorColumnSelector("v", DeferExpressionDimensions.ALWAYS);
        while (!cursor.isDone()) {
          final List<Object> resultsVector = new ArrayList<>();
          boolean[] nulls;
          switch (outputType.getType()) {
            case LONG:
              Assertions.assertNotNull(selector);
              nulls = selector.getNullVector();
              long[] longs = selector.getLongVector();
              for (int i = 0; i < selector.getCurrentVectorSize(); i++, rowCount++) {
                resultsVector.add(nulls != null && nulls[i] ? null : longs[i]);
              }
              break;
            case DOUBLE:
              Assertions.assertNotNull(selector);
              // special case to test floats just to get coverage on getFloatVector
              if ("float2".equals(expression)) {
                nulls = selector.getNullVector();
                float[] floats = selector.getFloatVector();
                for (int i = 0; i < selector.getCurrentVectorSize(); i++, rowCount++) {
                  resultsVector.add(nulls != null && nulls[i] ? null : (double) floats[i]);
                }
              } else {
                nulls = selector.getNullVector();
                double[] doubles = selector.getDoubleVector();
                for (int i = 0; i < selector.getCurrentVectorSize(); i++, rowCount++) {
                  resultsVector.add(nulls != null && nulls[i] ? null : doubles[i]);
                }
              }
              break;
            default:
              Assertions.assertNotNull(objectSelector);
              Object[] objects = objectSelector.getObjectVector();
              for (int i = 0; i < objectSelector.getCurrentVectorSize(); i++, rowCount++) {
                resultsVector.add(objects[i]);
              }
              break;
          }

          verifyGroupBySelector(groupBySelector, resultsVector);
          results.addAll(resultsVector);
          cursor.advance();
        }
      }


      final Cursor nonVectorized = cursorHolder.asCursor();
      Assertions.assertNotNull(nonVectorized);
      final ColumnValueSelector nonSelector = nonVectorized.getColumnSelectorFactory()
                                                           .makeColumnValueSelector("v");
      int rows = 0;
      while (!nonVectorized.isDone()) {
        Assertions.assertEquals(
            nonSelector.getObject(),
            results.get(rows),
            "Failed at row " + rows
        );
        rows++;
        nonVectorized.advance();
      }

      Assertions.assertTrue(rows > 0);
      Assertions.assertEquals(rows, rowCount);
    }
  }

  private static void verifyGroupBySelector(
      final GroupByVectorColumnSelector groupBySelector,
      final List<Object> expectedResults
  )
  {
    final int keyOffset = 1;
    final int keySize = groupBySelector.getGroupingKeySize() + keyOffset + 1; // 1 byte before, 1 byte after
    final WritableMemory keySpace =
        WritableMemory.allocate(keySize * expectedResults.size());

    final int writeKeysRetVal = groupBySelector.writeKeys(keySpace, keySize, keyOffset, 0, expectedResults.size());
    Assertions.assertEquals(0, writeKeysRetVal);

    for (int i = 0; i < expectedResults.size(); i++) {
      final ResultRow resultRow = ResultRow.create(1);
      groupBySelector.writeKeyToResultRow(new MemoryPointer(keySpace, (long) keySize * i), keyOffset, resultRow, 0);
      Assertions.assertEquals(expectedResults.get(i), resultRow.getArray()[0], "row #" + i);
    }
  }
}
