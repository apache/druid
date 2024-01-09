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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class LikeFilterTest extends BaseFilterTest
{
  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", DateTimes.of("2000")),
          DimensionsSpec.builder()
                        .setDimensions(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim0", "dim1", "dim2")))
                        .build()
      )
  );
  private static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                                .add("dim0", ColumnType.STRING)
                                                                .add("dim1", ColumnType.STRING)
                                                                .add("dim2", ColumnType.STRING)
                                                                .build();

  private static final List<InputRow> ROWS = ImmutableList.of(
      makeSchemaRow(PARSER, ROW_SIGNATURE, "0", "", ""),
      makeSchemaRow(PARSER, ROW_SIGNATURE, "1", "foo", "aaa"),
      makeSchemaRow(PARSER, ROW_SIGNATURE, "2", "foobar", "aab"),
      makeSchemaRow(PARSER, ROW_SIGNATURE, "3", "bar", null),
      makeSchemaRow(PARSER, ROW_SIGNATURE, "4", "foobarbaz", "abb"),
      makeSchemaRow(PARSER, ROW_SIGNATURE, "5", "foo%bar", "bbb"),
      makeSchemaRow(PARSER, ROW_SIGNATURE, "6", "new\nline", "bbz")
  );

  public LikeFilterTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean cnf,
      boolean optimize
  )
  {
    super(testName, ROWS, indexBuilder, finisher, cnf, optimize);
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    BaseFilterTest.tearDown(LikeFilterTest.class.getName());
  }

  @Test
  public void testExactMatch()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "bar", null, null),
        ImmutableList.of("3")
    );
    assertFilterMatches(
        NotDimFilter.of(new LikeDimFilter("dim1", "bar", null, null)),
        ImmutableList.of("0", "1", "2", "4", "5", "6")
    );

    assertFilterMatches(
        new LikeDimFilter("dim2", "bbb", null, null),
        ImmutableList.of("5")
    );
    assertFilterMatches(
        NotDimFilter.of(new LikeDimFilter("dim2", "bbb", null, null)),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of("0", "1", "2", "3", "4", "6")
        : ImmutableList.of("0", "1", "2", "4", "6")
    );
  }

  @Test
  public void testExactMatchWithEscape()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "@bar", "@", null),
        ImmutableList.of("3")
    );
  }

  @Test
  public void testExactMatchWithExtractionFn()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "bar", null, new SubstringDimExtractionFn(3, 3)),
        ImmutableList.of("2", "4")
    );
    assertFilterMatches(
        NotDimFilter.of(new LikeDimFilter("dim1", "bar", null, new SubstringDimExtractionFn(3, 3))),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of("0", "1", "3", "5", "6")
        : ImmutableList.of("5", "6")
    );

    assertFilterMatches(
        new LikeDimFilter("dim2", "bbb", null, new SubstringDimExtractionFn(0, 3)),
        ImmutableList.of("5")
    );
    assertFilterMatches(
        NotDimFilter.of(new LikeDimFilter("dim2", "bbb", null, new SubstringDimExtractionFn(0, 3))),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of("0", "1", "2", "3", "4", "6")
        : ImmutableList.of("1", "2", "4", "6")
    );
  }

  @Test
  public void testPrefixMatch()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "foo%", null, null),
        ImmutableList.of("1", "2", "4", "5")
    );
    assertFilterMatches(
        NotDimFilter.of(new LikeDimFilter("dim1", "foo%", null, null)),
        ImmutableList.of("0", "3", "6")
    );

    assertFilterMatches(
        new LikeDimFilter("dim2", "aa%", null, null),
        ImmutableList.of("1", "2")
    );
    assertFilterMatches(
        NotDimFilter.of(new LikeDimFilter("dim2", "aa%", null, null)),
        NullHandling.replaceWithDefault() ? ImmutableList.of("0", "3", "4", "5", "6") : ImmutableList.of("0", "4", "5", "6")
    );
  }

  @Test
  public void testPrefixMatchWithEscape()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "foo@%%", "@", null),
        ImmutableList.of("5")
    );
  }

  @Test
  public void testPrefixMatchWithExtractionFn()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "a%", null, new SubstringDimExtractionFn(1, null)),
        ImmutableList.of("3")
    );
    assertFilterMatches(
        NotDimFilter.of(new LikeDimFilter("dim1", "a%", null, new SubstringDimExtractionFn(1, null))),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of("0", "1", "2", "4", "5", "6")
        : ImmutableList.of("1", "2", "4", "5", "6")
    );

    assertFilterMatches(
        new LikeDimFilter("dim2", "a%", null, new SubstringDimExtractionFn(1, null)),
        ImmutableList.of("1", "2")
    );
    assertFilterMatches(
        NotDimFilter.of(new LikeDimFilter("dim2", "a%", null, new SubstringDimExtractionFn(1, null))),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of("0", "3", "4", "5", "6")
        : ImmutableList.of("4", "5", "6")
    );
  }

  @Test
  public void testWildcardMatch()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "%oba%", null, null),
        ImmutableList.of("2", "4")
    );
    assertFilterMatches(
        NotDimFilter.of(new LikeDimFilter("dim1", "%oba%", null, null)),
        ImmutableList.of("0", "1", "3", "5", "6")
    );

    assertFilterMatches(
        new LikeDimFilter("dim2", "%b%", null, null),
        ImmutableList.of("2", "4", "5", "6")
    );
    assertFilterMatches(
        NotDimFilter.of(new LikeDimFilter("dim2", "%b%", null, null)),
        NullHandling.sqlCompatible() ? ImmutableList.of("0", "1") : ImmutableList.of("0", "1", "3")
    );
  }

  @Test
  public void testMatchEmptyString()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "", null, null),
        ImmutableList.of("0")
    );
  }

  @Test
  public void testMatchEmptyStringWithExtractionFn()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          new LikeDimFilter("dim1", "", null, new SubstringDimExtractionFn(100, 1)),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6")
      );
    } else {
      assertFilterMatches(
          new LikeDimFilter("dim1", "", null, new SubstringDimExtractionFn(100, 1)),
          ImmutableList.of()
      );
    }
  }

  @Test
  public void testWildcardMatchWithEscape()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "%@%ba%", "@", null),
        ImmutableList.of("5")
    );
  }

  @Test
  public void testWildcardMatchEverything()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "%", "@", null),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6")
    );
    assertFilterMatches(
        NotDimFilter.of(new LikeDimFilter("dim1", "%", "@", null)),
        ImmutableList.of()
    );

    // doesnt match null tho in sql compatible mode
    assertFilterMatches(
        new LikeDimFilter("dim2", "%", "@", null),
        NullHandling.sqlCompatible()
        ? ImmutableList.of("0", "1", "2", "4", "5", "6")
        : ImmutableList.of("0", "1", "2", "3", "4", "5", "6")
    );
    assertFilterMatches(
        NotDimFilter.of(new LikeDimFilter("dim2", "%", "@", null)),
        ImmutableList.of()
    );
  }

  @Test
  public void testPrefixAndSuffixMatch()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "f%r", null, null),
        ImmutableList.of("2", "5")
    );
  }

  @Test
  public void testUnderscoreMatch()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "f_o", null, null),
        ImmutableList.of("1")
    );
  }

  @Test
  public void testEscapeEscapingItself()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "foooo", "o", null),
        ImmutableList.of("1")
    );
  }

  @Test
  public void testSuffixMatchWithExtractionFn()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "%ar", null, new SubstringDimExtractionFn(3, 3)),
        ImmutableList.of("2", "4")
    );
  }

  @Test
  public void testNewlineMatch()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "ne%", null, null),
        ImmutableList.of("6")
    );

    assertFilterMatches(
        new LikeDimFilter("dim1", "%ine", null, null),
        ImmutableList.of("6")
    );

    assertFilterMatches(
        new LikeDimFilter("dim1", "new_line", null, null),
        ImmutableList.of("6")
    );
  }

  @Test
  public void testNewlineMatchWithExtractionFn()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "e%", null, new SubstringDimExtractionFn(1, 100)),
        ImmutableList.of("6")
    );

    assertFilterMatches(
        new LikeDimFilter("dim1", "%ine", null, new SubstringDimExtractionFn(1, 100)),
        ImmutableList.of("6")
    );

    assertFilterMatches(
        new LikeDimFilter("dim1", "ew_line", null, new SubstringDimExtractionFn(1, 100)),
        ImmutableList.of("6")
    );
  }

  @Test
  public void testListFilteredVirtualColumn()
  {
    assertFilterMatchesSkipVectorize(
        new LikeDimFilter("allow-dim0", "1%", null, null),
        ImmutableList.of()
    );
    assertFilterMatchesSkipVectorize(
        new LikeDimFilter("allow-dim0", "3%", null, null),
        ImmutableList.of("3")
    );
    assertFilterMatchesSkipVectorize(
        new LikeDimFilter("allow-dim0", "%3", null, null),
        ImmutableList.of("3")
    );
  }

  @Test
  public void testRequiredColumnRewrite()
  {
    Filter filter = new LikeDimFilter("dim0", "e%", null, new SubstringDimExtractionFn(1, 100)).toFilter();
    Filter filter2 = new LikeDimFilter("dim1", "e%", null, new SubstringDimExtractionFn(1, 100)).toFilter();

    Assert.assertTrue(filter.supportsRequiredColumnRewrite());
    Assert.assertTrue(filter2.supportsRequiredColumnRewrite());

    Filter rewrittenFilter = filter.rewriteRequiredColumns(ImmutableMap.of("dim0", "dim1"));
    Assert.assertEquals(filter2, rewrittenFilter);

    Throwable t = Assert.assertThrows(IAE.class, () -> filter.rewriteRequiredColumns(ImmutableMap.of("invalidName", "dim1")));
    Assert.assertEquals("Received a non-applicable rewrite: {invalidName=dim1}, filter's dimension: dim0", t.getMessage());
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(LikeFilter.class)
                  .usingGetClass()
                  .withNonnullFields("dimension", "likeMatcher")
                  .verify();
  }
}
