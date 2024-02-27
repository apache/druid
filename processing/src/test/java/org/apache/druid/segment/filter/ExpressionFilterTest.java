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

package org.apache.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class ExpressionFilterTest extends BaseFilterTest
{
  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", DateTimes.of("2000")),
          new DimensionsSpec(
              ImmutableList.of(
                  new StringDimensionSchema("dim0"),
                  new LongDimensionSchema("dim1"),
                  new FloatDimensionSchema("dim2"),
                  new StringDimensionSchema("dim3"),
                  new StringDimensionSchema("dim4"),
                  new StringDimensionSchema("dim5")
              )
          )
      )
  );

  private static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                                .add("dim0", ColumnType.STRING)
                                                                .add("dim1", ColumnType.LONG)
                                                                .add("dim2", ColumnType.FLOAT)
                                                                .add("dim3", ColumnType.STRING)
                                                                .add("dim4", ColumnType.STRING)
                                                                .add("dim5", ColumnType.STRING)
                                                                .build();


  private static final List<InputRow> ROWS = ImmutableList.of(
      makeSchemaRow(PARSER, ROW_SIGNATURE, "0", 0L, 0.0f, "", ImmutableList.of("1", "2"), "a"),
      makeSchemaRow(PARSER, ROW_SIGNATURE, "1", 1L, 1.0f, "10", ImmutableList.of(), "b"),
      makeSchemaRow(PARSER, ROW_SIGNATURE, "2", 2L, 2.0f, "2", ImmutableList.of(""), null),
      makeSchemaRow(PARSER, ROW_SIGNATURE, "3", 3L, 3.0f, "1", ImmutableList.of("3"), "c"),
      makeSchemaRow(PARSER, ROW_SIGNATURE, "4", 4L, 4.0f, "1", ImmutableList.of("4", "5"), ""),
      makeSchemaRow(PARSER, ROW_SIGNATURE, "5", 5L, 5.0f, "5", ImmutableList.of("4", "5"), "d"),
      makeSchemaRow(PARSER, ROW_SIGNATURE, "6", 6L, 6.0f, "1", null, "e"),
      makeSchemaRow(PARSER, ROW_SIGNATURE, "7", 7L, 7.0f, "a", null, "f"),
      makeSchemaRow(PARSER, ROW_SIGNATURE, "8", 8L, 8.0f, 8L, null, "g"),
      // Note: the "dim3 == 1.234" check in "testOneSingleValuedStringColumn" fails if dim3 is 1.234f instead of 1.234d,
      // because the literal 1.234 is interpreted as a double, and 1.234f cast to double is not equivalent to 1.234d.
      makeSchemaRow(PARSER, ROW_SIGNATURE, "9", 9L, 9.0f, 1.234d, 1.234d, null)
  );

  public ExpressionFilterTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean cnf,
      boolean optimize
  )
  {
    super(
        testName,
        ROWS,
        indexBuilder.schema(
            new IncrementalIndexSchema.Builder()
                .withDimensionsSpec(PARSER.getParseSpec().getDimensionsSpec()).build()
        ),
        finisher,
        cnf,
        optimize
    );
  }

  @Before
  public void setup()
  {
    ExpressionProcessing.initializeForStrictBooleansTests(true);
  }

  @After
  public void teardown()
  {
    ExpressionProcessing.initializeForTests();
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    BaseFilterTest.tearDown(ColumnComparisonFilterTest.class.getName());
  }

  @Test
  public void testOneSingleValuedStringColumn()
  {
    if (testName.contains("incrementalAutoTypes")) {
      // dim 3 is mixed type in auto incrementalIndex, so presents as complex<json>
      return;
    }
    assertFilterMatches(edf("dim3 == ''"), ImmutableList.of("0"));
    assertFilterMatches(edf("dim3 == '1'"), ImmutableList.of("3", "4", "6"));
    assertFilterMatches(edf("dim3 == 'a'"), ImmutableList.of("7"));
    assertFilterMatches(edf("dim3 == 1"), ImmutableList.of("3", "4", "6"));
    assertFilterMatches(edf("dim3 == 1.0"), ImmutableList.of("3", "4", "6"));
    assertFilterMatches(edf("dim3 == 1.234"), ImmutableList.of("9"));
    assertFilterMatches(edf("dim3 < '2'"), ImmutableList.of("0", "1", "3", "4", "6", "9"));
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(edf("dim3 < 2"), ImmutableList.of("0", "3", "4", "6", "7", "9"));
      assertFilterMatches(edf("dim3 < 2.0"), ImmutableList.of("0", "3", "4", "6", "7", "9"));
    } else {
      // Empty String and "a" will not match
      assertFilterMatches(edf("dim3 < 2"), ImmutableList.of("3", "4", "6", "9"));
      assertFilterMatches(edf("dim3 < 2.0"), ImmutableList.of("3", "4", "6", "9"));
    }
    assertFilterMatchesSkipVectorize(edf("like(dim3, '1%')"), ImmutableList.of("1", "3", "4", "6", "9"));
    assertFilterMatchesSkipVectorize(edf("array_contains(dim3, '1')"), ImmutableList.of("3", "4", "6"));
  }

  @Test
  public void testComplement()
  {
    if (NullHandling.sqlCompatible()) {
      assertFilterMatches(edf("dim5 == 'a'"), ImmutableList.of("0"));
      assertFilterMatches(
          NotDimFilter.of(edf("dim5 == 'a'")),
          ImmutableList.of("1", "3", "4", "5", "6", "7", "8")
      );
      assertFilterMatches(
          edf("dim5 == ''"), ImmutableList.of("4")
      );
      assertFilterMatches(
          NotDimFilter.of(edf("dim5 == ''")), ImmutableList.of("0", "1", "3", "5", "6", "7", "8")
      );
    } else {
      assertFilterMatches(edf("dim5 == 'a'"), ImmutableList.of("0"));
      assertFilterMatches(
          NotDimFilter.of(edf("dim5 == 'a'")),
          NullHandling.sqlCompatible()
          ? ImmutableList.of("1", "3", "5", "6", "7", "8")
          : ImmutableList.of("1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
    }
  }

  @Test
  public void testOneMultiValuedStringColumn()
  {
    // auto type columns ingest arrays instead of mvds, bail out
    if (isAutoSchema()) {
      return;
    }
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatchesSkipVectorize(edf("dim4 == ''"), ImmutableList.of("1", "2", "6", "7", "8"));
    } else {
      assertFilterMatchesSkipVectorize(edf("dim4 == ''"), ImmutableList.of("2"));
      // AS per SQL standard null == null returns false.
      assertFilterMatchesSkipVectorize(edf("dim4 == null"), ImmutableList.of());
    }
    assertFilterMatchesSkipVectorize(edf("dim4 == '1'"), ImmutableList.of("0"));
    assertFilterMatchesSkipVectorize(edf("dim4 == '3'"), ImmutableList.of("3"));
    assertFilterMatchesSkipVectorize(edf("dim4 == '4'"), ImmutableList.of("4", "5"));
    assertFilterMatchesSkipVectorize(edf("concat(dim4, dim4) == '33'"), ImmutableList.of("3"));
    assertFilterMatchesSkipVectorize(edf("like(dim4, '4%')"), ImmutableList.of("4", "5"));
    assertFilterMatchesSkipVectorize(edf("array_contains(dim4, '5')"), ImmutableList.of("4", "5"));
    assertFilterMatchesSkipVectorize(edf("array_to_string(dim4, ':') == '4:5'"), ImmutableList.of("4", "5"));
  }

  @Test
  public void testSingleAndMultiValuedStringColumn()
  {
    assertFilterMatchesSkipVectorize(edf("array_contains(dim4, dim3)"), ImmutableList.of("5", "9"));
  }

  @Test
  public void testOneLongColumn()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(edf("dim1 == ''"), ImmutableList.of("0"));
    } else {
      // A long does not match empty string
      assertFilterMatches(edf("dim1 == ''"), ImmutableList.of());
    }
    assertFilterMatches(edf("dim1 == '1'"), ImmutableList.of("1"));
    assertFilterMatches(edf("dim1 == 2"), ImmutableList.of("2"));
    assertFilterMatches(edf("dim1 < '2'"), ImmutableList.of("0", "1"));
    assertFilterMatches(edf("dim1 < 2"), ImmutableList.of("0", "1"));
    assertFilterMatches(edf("dim1 < 2.0"), ImmutableList.of("0", "1"));
    assertFilterMatchesSkipVectorize(edf("like(dim1, '1%')"), ImmutableList.of("1"));
  }

  @Test
  public void testOneFloatColumn()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(edf("dim2 == ''"), ImmutableList.of("0"));
    } else {
      // A float does not match empty string
      assertFilterMatches(edf("dim2 == ''"), ImmutableList.of());
    }
    assertFilterMatches(edf("dim2 == '1'"), ImmutableList.of("1"));
    assertFilterMatches(edf("dim2 == 2"), ImmutableList.of("2"));
    assertFilterMatches(edf("dim2 < '2'"), ImmutableList.of("0", "1"));
    assertFilterMatches(edf("dim2 < 2"), ImmutableList.of("0", "1"));
    assertFilterMatches(edf("dim2 < 2.0"), ImmutableList.of("0", "1"));
    assertFilterMatchesSkipVectorize(edf("like(dim2, '1%')"), ImmutableList.of("1"));
  }

  @Test
  public void testConstantExpression()
  {
    assertFilterMatches(edf("1 + 1"), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    assertFilterMatches(edf("'true'"), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));

    assertFilterMatches(edf("0 + 0"), ImmutableList.of());
    assertFilterMatches(edf("'false'"), ImmutableList.of());
  }

  @Test
  public void testCompareColumns()
  {
    // String vs string
    assertFilterMatches(edf("dim0 == dim3"), ImmutableList.of("2", "5", "8"));

    if (NullHandling.replaceWithDefault()) {
      // String vs long
      assertFilterMatches(edf("dim1 == dim3"), ImmutableList.of("0", "2", "5", "8"));

      // String vs float
      assertFilterMatches(edf("dim2 == dim3"), ImmutableList.of("0", "2", "5", "8"));
    } else {
      // String vs long
      assertFilterMatches(edf("dim1 == dim3"), ImmutableList.of("2", "5", "8"));

      // String vs float
      assertFilterMatches(edf("dim2 == dim3"), ImmutableList.of("2", "5", "8"));
    }

    // auto type columns ingest arrays instead of mvds
    if (isAutoSchema()) {
      return;
    }
    // String vs. multi-value string
    assertFilterMatchesSkipVectorize(edf("dim0 == dim4"), ImmutableList.of("3", "4", "5"));
  }

  @Test
  public void testNullNotUnknown()
  {
    assertFilterMatchesSkipVectorize(
        edf("isfalse(dim5)"),
        NullHandling.sqlCompatible() ? ImmutableList.of("0", "1", "3", "4", "5", "6", "7", "8") : ImmutableList.of("0", "1", "3", "5", "6", "7", "8")
    );
    assertFilterMatchesSkipVectorize(
        edf("!isfalse(dim5)"),
        NullHandling.sqlCompatible() ? ImmutableList.of("2", "9") : ImmutableList.of("2", "4", "9")
    );
    assertFilterMatchesSkipVectorize(
        NotDimFilter.of(edf("isfalse(dim5)")),
        NullHandling.sqlCompatible() ? ImmutableList.of("2", "9") : ImmutableList.of("2", "4", "9")
    );

    assertFilterMatchesSkipVectorize(
        edf("isfalse(notexist)"),
        ImmutableList.of()
    );
    assertFilterMatchesSkipVectorize(
        edf("!isfalse(notexist)"), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );
    assertFilterMatchesSkipVectorize(
        NotDimFilter.of(edf("isfalse(notexist)")), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );
  }

  @Test
  public void testMissingColumn()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          edf("missing == ''"),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
      assertFilterMatches(
          edf("missing == otherMissing"),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
    } else {
      // AS per SQL standard null == null returns false.
      assertFilterMatches(edf("missing == null"), ImmutableList.of());
      // and inverted too
      assertFilterMatches(NotDimFilter.of(edf("missing == null")), ImmutableList.of());
      // also this madness doesn't do madness
      assertFilterMatches(
          edf("missing == otherMissing"),
          ImmutableList.of()
      );
      assertFilterMatches(
          NotDimFilter.of(edf("missing == otherMissing")),
          ImmutableList.of()
      );
    }
    assertFilterMatches(edf("missing == '1'"), ImmutableList.of());
    assertFilterMatches(edf("missing == 2"), ImmutableList.of());
    if (NullHandling.replaceWithDefault()) {
      // missing equivaluent to 0
      assertFilterMatches(
          edf("missing < '2'"),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
      assertFilterMatches(
          edf("missing < 2"),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
      assertFilterMatches(
          edf("missing < 2.0"),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
    } else {
      // missing equivalent to null
      assertFilterMatches(edf("missing < '2'"), ImmutableList.of());
      assertFilterMatches(edf("missing < 2"), ImmutableList.of());
      assertFilterMatches(edf("missing < 2.0"), ImmutableList.of());
    }
    assertFilterMatches(edf("missing > '2'"), ImmutableList.of());
    assertFilterMatches(edf("missing > 2"), ImmutableList.of());
    assertFilterMatches(edf("missing > 2.0"), ImmutableList.of());
    assertFilterMatchesSkipVectorize(edf("like(missing, '1%')"), ImmutableList.of());
  }

  @Test
  public void testGetRequiredColumn()
  {
    Assert.assertEquals(edf("like(dim1, '1%')").getRequiredColumns(), Sets.newHashSet("dim1"));
    Assert.assertEquals(edf("dim2 == '1'").getRequiredColumns(), Sets.newHashSet("dim2"));
    Assert.assertEquals(edf("dim3 < '2'").getRequiredColumns(), Sets.newHashSet("dim3"));
    Assert.assertEquals(edf("dim4 == ''").getRequiredColumns(), Sets.newHashSet("dim4"));
    Assert.assertEquals(edf("1 + 1").getRequiredColumns(), new HashSet<>());
    Assert.assertEquals(edf("dim0 == dim3").getRequiredColumns(), Sets.newHashSet("dim0", "dim3"));
    Assert.assertEquals(edf("missing == ''").getRequiredColumns(), Sets.newHashSet("missing"));
  }

  @Test
  public void testEqualsContract()
  {
    EqualsVerifier.forClass(ExpressionFilter.class)
                  .withIgnoredFields("bindingDetails")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testRequiredColumnRewrite()
  {
    Filter filter = edf("dim1 == '1'").toFilter();
    Assert.assertFalse(filter.supportsRequiredColumnRewrite());

    Throwable t = Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> filter.rewriteRequiredColumns(ImmutableMap.of("invalidName", "dim1"))
    );
    Assert.assertEquals("Required column rewrite is not supported by this filter.", t.getMessage());
  }

  protected static ExpressionDimFilter edf(final String expression)
  {
    return new ExpressionDimFilter(expression, null, TestExprMacroTable.INSTANCE);
  }
}
