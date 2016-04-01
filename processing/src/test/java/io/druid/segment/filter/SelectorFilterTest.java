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
import com.metamx.common.Pair;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.segment.IndexBuilder;
import io.druid.segment.StorageAdapter;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class SelectorFilterTest extends BaseFilterTest
{
  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", new DateTime("2000")),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim0", "dim1", "dim2", "dim3")),
              null,
              null
          )
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "0", "dim1", "", "dim2", ImmutableList.of("a", "b"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "1", "dim1", "10", "dim2", ImmutableList.of())),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "2", "dim1", "2", "dim2", ImmutableList.of(""))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "3", "dim1", "1", "dim2", ImmutableList.of("a"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "4", "dim1", "def", "dim2", ImmutableList.of("c"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "5", "dim1", "abc"))
  );

  public SelectorFilterTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean optimize
  )
  {
    super(ROWS, indexBuilder, finisher, optimize);
  }

  @Test
  public void testSingleValueStringColumnWithoutNulls()
  {
    assertFilterMatches(new SelectorDimFilter("dim0", null), ImmutableList.<String>of());
    assertFilterMatches(new SelectorDimFilter("dim0", ""), ImmutableList.<String>of());
    assertFilterMatches(new SelectorDimFilter("dim0", "0"), ImmutableList.of("0"));
    assertFilterMatches(new SelectorDimFilter("dim0", "1"), ImmutableList.of("1"));
  }

  @Test
  public void testSingleValueStringColumnWithNulls()
  {
    assertFilterMatches(new SelectorDimFilter("dim1", null), ImmutableList.of("0"));
    assertFilterMatches(new SelectorDimFilter("dim1", ""), ImmutableList.of("0"));
    assertFilterMatches(new SelectorDimFilter("dim1", "10"), ImmutableList.of("1"));
    assertFilterMatches(new SelectorDimFilter("dim1", "2"), ImmutableList.of("2"));
    assertFilterMatches(new SelectorDimFilter("dim1", "1"), ImmutableList.of("3"));
    assertFilterMatches(new SelectorDimFilter("dim1", "def"), ImmutableList.of("4"));
    assertFilterMatches(new SelectorDimFilter("dim1", "abc"), ImmutableList.of("5"));
    assertFilterMatches(new SelectorDimFilter("dim1", "ab"), ImmutableList.<String>of());
  }

  @Test
  public void testMultiValueStringColumn()
  {
    assertFilterMatches(new SelectorDimFilter("dim2", null), ImmutableList.of("1", "2", "5"));
    assertFilterMatches(new SelectorDimFilter("dim2", ""), ImmutableList.of("1", "2", "5"));
    assertFilterMatches(new SelectorDimFilter("dim2", "a"), ImmutableList.of("0", "3"));
    assertFilterMatches(new SelectorDimFilter("dim2", "b"), ImmutableList.of("0"));
    assertFilterMatches(new SelectorDimFilter("dim2", "c"), ImmutableList.of("4"));
    assertFilterMatches(new SelectorDimFilter("dim2", "d"), ImmutableList.<String>of());
  }

  @Test
  public void testMissingColumnSpecifiedInDimensionList()
  {
    assertFilterMatches(new SelectorDimFilter("dim3", null), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new SelectorDimFilter("dim3", ""), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new SelectorDimFilter("dim3", "a"), ImmutableList.<String>of());
    assertFilterMatches(new SelectorDimFilter("dim3", "b"), ImmutableList.<String>of());
    assertFilterMatches(new SelectorDimFilter("dim3", "c"), ImmutableList.<String>of());
  }

  @Test
  public void testMissingColumnNotSpecifiedInDimensionList()
  {
    assertFilterMatches(new SelectorDimFilter("dim4", null), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new SelectorDimFilter("dim4", ""), ImmutableList.of("0", "1", "2", "3", "4", "5"));
    assertFilterMatches(new SelectorDimFilter("dim4", "a"), ImmutableList.<String>of());
    assertFilterMatches(new SelectorDimFilter("dim4", "b"), ImmutableList.<String>of());
    assertFilterMatches(new SelectorDimFilter("dim4", "c"), ImmutableList.<String>of());
  }

  private void assertFilterMatches(
      final DimFilter filter,
      final List<String> expectedRows
  )
  {
    Assert.assertEquals(filter.toString(), expectedRows, selectColumnValuesMatchingFilter(filter, "dim0"));
    Assert.assertEquals(filter.toString(), expectedRows.size(), selectCountUsingFilteredAggregator(filter));
  }
}
