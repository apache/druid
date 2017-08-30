/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Pair;
import io.druid.query.extraction.SubstringDimExtractionFn;
import io.druid.query.filter.LikeDimFilter;
import io.druid.segment.IndexBuilder;
import io.druid.segment.StorageAdapter;
import org.junit.AfterClass;
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
          new DimensionsSpec(null, null, null)
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "0", "dim1", "")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "1", "dim1", "foo")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "2", "dim1", "foobar")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "3", "dim1", "bar")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "4", "dim1", "foobarbaz")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "5", "dim1", "foo%bar"))
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
  }

  @Test
  public void testPrefixMatch()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "foo%", null, null),
        ImmutableList.of("1", "2", "4", "5")
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
  }

  @Test
  public void testWildcardMatch()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "%oba%", null, null),
        ImmutableList.of("2", "4")
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
    assertFilterMatches(
        new LikeDimFilter("dim1", "", null, new SubstringDimExtractionFn(100, 1)),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
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
        ImmutableList.of("0", "1", "2", "3", "4", "5")
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
  public void testSuffixMatchWithExtractionFn()
  {
    assertFilterMatches(
        new LikeDimFilter("dim1", "%ar", null, new SubstringDimExtractionFn(3, 3)),
        ImmutableList.of("2", "4")
    );
  }
}
