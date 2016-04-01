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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.Pair;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.Filter;
import io.druid.query.filter.InDimFilter;
import io.druid.segment.IndexBuilder;
import io.druid.segment.StorageAdapter;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class InFilterTest extends BaseFilterTest
{
  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", new DateTime("2000")),
          new DimensionsSpec(null, null, null)
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "a", "dim1", "", "dim2", ImmutableList.of("a", "b"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "b", "dim1", "10", "dim2", ImmutableList.of())),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "c", "dim1", "2", "dim2", ImmutableList.of(""))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "d", "dim1", "1", "dim2", ImmutableList.of("a"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "e", "dim1", "def", "dim2", ImmutableList.of("c"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "f", "dim1", "abc"))
  );

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder() throws IOException
  {
    return makeConstructors();
  }

  public InFilterTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean optimize
  )
  {
    super(ROWS, indexBuilder, finisher, optimize);
  }

  @Before
  public void setUp() throws IOException
  {
    final Pair<StorageAdapter, Closeable> pair = finisher.apply(
        indexBuilder.tmpDir(temporaryFolder.newFolder()).add(ROWS)
    );
    this.adapter = pair.lhs;
    this.closeable = pair.rhs;
  }

  @Test
  public void testSingleValueStringColumnWithoutNulls()
  {
    Assert.assertEquals(ImmutableList.<Integer>of(), select(toInFilter("dim0", null)));
    Assert.assertEquals(ImmutableList.<Integer>of(), select(toInFilter("dim0", "", "")));
    Assert.assertEquals(ImmutableList.of(0, 2), select(toInFilter("dim0", "a", "c")));
    Assert.assertEquals(ImmutableList.of(4), select(toInFilter("dim0", "e", "x")));
  }

  @Test
  public void testSingleValueStringColumnWithNulls()
  {
    Assert.assertEquals(ImmutableList.of(0), select(toInFilter("dim1", null, "")));
    Assert.assertEquals(ImmutableList.of(0), select(toInFilter("dim1", "")));
    Assert.assertEquals(ImmutableList.of(0, 1, 5), select(toInFilter("dim1", null, "10", "abc")));
    Assert.assertEquals(ImmutableList.<Integer>of(), select(toInFilter("dim1", "-1", "ab", "de")));
  }

  @Test
  public void testMultiValueStringColumn()
  {
    Assert.assertEquals(ImmutableList.of(1, 2, 5), select(toInFilter("dim2", null)));
    Assert.assertEquals(ImmutableList.of(1, 2, 5), select(toInFilter("dim2", "", (String)null)));
    Assert.assertEquals(ImmutableList.of(0, 1, 2, 3, 5), select(toInFilter("dim2", null, "a")));
    Assert.assertEquals(ImmutableList.of(0, 1, 2, 5), select(toInFilter("dim2", null, "b")));
    Assert.assertEquals(ImmutableList.of(4), select(toInFilter("dim2", "c")));
    Assert.assertEquals(ImmutableList.<Integer>of(), select(toInFilter("dim2", "d")));
  }

  @Test
  public void testMissingColumn()
  {
    Assert.assertEquals(ImmutableList.of(0, 1, 2, 3, 4, 5), select(toInFilter("dim3", null, (String)null)));
    Assert.assertEquals(ImmutableList.of(0, 1, 2, 3, 4, 5), select(toInFilter("dim3", "")));
    Assert.assertEquals(ImmutableList.of(0, 1, 2, 3, 4, 5), select(toInFilter("dim3", null, "a")));
    Assert.assertEquals(ImmutableList.<Integer>of(), select(toInFilter("dim3", "a")));
    Assert.assertEquals(ImmutableList.<Integer>of(), select(toInFilter("dim3", "b")));
    Assert.assertEquals(ImmutableList.<Integer>of(), select(toInFilter("dim3", "c")));
  }

  private DimFilter toInFilter(String dim, String value, String... values)
  {
    return new InDimFilter(dim, Lists.asList(value, values));
  }

  private List<Integer> select(final DimFilter filter)
  {
    return Lists.newArrayList(
        Iterables.transform(
            selectColumnValuesMatchingFilter(filter, "dim0"),
            new Function<String, Integer>()
            {
              @Override
              public Integer apply(String input)
              {
                Preconditions.checkArgument(input.length() == 1);
                return ((int) input.charAt(0)) - ((int) 'a');
              }
            }
        )
    );
  }
}
