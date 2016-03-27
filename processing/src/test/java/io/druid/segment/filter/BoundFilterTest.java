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
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.Filter;
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
public class BoundFilterTest extends BaseFilterTest
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

  private final IndexBuilder indexBuilder;
  private final Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher;

  public BoundFilterTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher
  )
  {
    this.indexBuilder = indexBuilder;
    this.finisher = finisher;
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

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder() throws IOException
  {
    return makeConstructors();
  }

  @Test
  public void testLexicographicMatchEverything()
  {
    final List<BoundFilter> filters = ImmutableList.of(
        new BoundFilter(new BoundDimFilter("dim0", "", "z", false, false, false)),
        new BoundFilter(new BoundDimFilter("dim1", "", "z", false, false, false)),
        new BoundFilter(new BoundDimFilter("dim2", "", "z", false, false, false)),
        new BoundFilter(new BoundDimFilter("dim3", "", "z", false, false, false))
    );

    for (BoundFilter filter : filters) {
      Assert.assertEquals(ImmutableList.of(0, 1, 2, 3, 4, 5), select(filter));
    }
  }

  @Test
  public void testLexicographicMatchNull()
  {
    Assert.assertEquals(
        ImmutableList.of(),
        select(new BoundFilter(new BoundDimFilter("dim0", "", "", false, false, false)))
    );
    Assert.assertEquals(
        ImmutableList.of(0),
        select(new BoundFilter(new BoundDimFilter("dim1", "", "", false, false, false)))
    );
    Assert.assertEquals(
        ImmutableList.of(1, 2, 5),
        select(new BoundFilter(new BoundDimFilter("dim2", "", "", false, false, false)))
    );
  }

  @Test
  public void testLexicographicMatchMissingColumn()
  {
    Assert.assertEquals(
        ImmutableList.of(0, 1, 2, 3, 4, 5),
        select(new BoundFilter(new BoundDimFilter("dim3", "", "", false, false, false)))
    );
    Assert.assertEquals(
        ImmutableList.of(),
        select(new BoundFilter(new BoundDimFilter("dim3", "", "", true, false, false)))
    );
    Assert.assertEquals(
        ImmutableList.of(),
        select(new BoundFilter(new BoundDimFilter("dim3", "", "", false, true, false)))
    );
    Assert.assertEquals(
        ImmutableList.of(0, 1, 2, 3, 4, 5),
        select(new BoundFilter(new BoundDimFilter("dim3", "", null, false, true, false)))
    );
    Assert.assertEquals(
        ImmutableList.of(0, 1, 2, 3, 4, 5),
        select(new BoundFilter(new BoundDimFilter("dim3", null, "", false, false, false)))
    );
    Assert.assertEquals(
        ImmutableList.of(),
        select(new BoundFilter(new BoundDimFilter("dim3", null, "", false, true, false)))
    );
  }

  @Test
  public void testLexicographicMatchTooStrict()
  {
    Assert.assertEquals(
        ImmutableList.of(),
        select(new BoundFilter(new BoundDimFilter("dim1", "abc", "abc", true, false, false)))
    );
    Assert.assertEquals(
        ImmutableList.of(),
        select(new BoundFilter(new BoundDimFilter("dim1", "abc", "abc", true, true, false)))
    );
    Assert.assertEquals(
        ImmutableList.of(),
        select(new BoundFilter(new BoundDimFilter("dim1", "abc", "abc", false, true, false)))
    );
  }

  @Test
  public void testLexicographicMatchExactlySingleValue()
  {
    Assert.assertEquals(
        ImmutableList.of(5),
        select(new BoundFilter(new BoundDimFilter("dim1", "abc", "abc", false, false, false)))
    );
  }

  @Test
  public void testLexicographicMatchSurroundingSingleValue()
  {
    Assert.assertEquals(
        ImmutableList.of(5),
        select(new BoundFilter(new BoundDimFilter("dim1", "ab", "abd", true, true, false)))
    );
  }

  @Test
  public void testLexicographicMatchNoUpperLimit()
  {
    Assert.assertEquals(
        ImmutableList.of(4, 5),
        select(new BoundFilter(new BoundDimFilter("dim1", "ab", null, true, true, false)))
    );
  }

  @Test
  public void testLexicographicMatchNoLowerLimit()
  {
    Assert.assertEquals(
        ImmutableList.of(0, 1, 2, 3, 5),
        select(new BoundFilter(new BoundDimFilter("dim1", null, "abd", true, true, false)))
    );
  }

  @Test
  public void testLexicographicMatchNumbers()
  {
    Assert.assertEquals(
        ImmutableList.of(1, 2, 3),
        select(new BoundFilter(new BoundDimFilter("dim1", "1", "3", false, false, false)))
    );
    Assert.assertEquals(
        ImmutableList.of(1, 2),
        select(new BoundFilter(new BoundDimFilter("dim1", "1", "3", true, true, false)))
    );
  }

  @Test
  public void testAlphaNumericMatchNull()
  {
    Assert.assertEquals(
        ImmutableList.of(),
        select(new BoundFilter(new BoundDimFilter("dim0", "", "", false, false, true)))
    );
    Assert.assertEquals(
        ImmutableList.of(0),
        select(new BoundFilter(new BoundDimFilter("dim1", "", "", false, false, true)))
    );
    Assert.assertEquals(
        ImmutableList.of(1, 2, 5),
        select(new BoundFilter(new BoundDimFilter("dim2", "", "", false, false, true)))
    );
    Assert.assertEquals(
        ImmutableList.of(0, 1, 2, 3, 4, 5),
        select(new BoundFilter(new BoundDimFilter("dim3", "", "", false, false, true)))
    );
  }

  @Test
  public void testAlphaNumericMatchTooStrict()
  {
    Assert.assertEquals(
        ImmutableList.of(),
        select(new BoundFilter(new BoundDimFilter("dim1", "2", "2", true, false, true)))
    );
    Assert.assertEquals(
        ImmutableList.of(),
        select(new BoundFilter(new BoundDimFilter("dim1", "2", "2", true, true, true)))
    );
    Assert.assertEquals(
        ImmutableList.of(),
        select(new BoundFilter(new BoundDimFilter("dim1", "2", "2", false, true, true)))
    );
  }

  @Test
  public void testAlphaNumericMatchExactlySingleValue()
  {
    Assert.assertEquals(
        ImmutableList.of(2),
        select(new BoundFilter(new BoundDimFilter("dim1", "2", "2", false, false, true)))
    );
  }

  @Test
  public void testAlphaNumericMatchSurroundingSingleValue()
  {
    Assert.assertEquals(
        ImmutableList.of(2),
        select(new BoundFilter(new BoundDimFilter("dim1", "1", "3", true, true, true)))
    );
  }

  @Test
  public void testAlphaNumericMatchNoUpperLimit()
  {
    Assert.assertEquals(
        ImmutableList.of(1, 2, 4, 5),
        select(new BoundFilter(new BoundDimFilter("dim1", "1", null, true, true, true)))
    );
  }

  @Test
  public void testAlphaNumericMatchNoLowerLimit()
  {
    Assert.assertEquals(
        ImmutableList.of(0, 3),
        select(new BoundFilter(new BoundDimFilter("dim1", null, "2", true, true, true)))
    );
  }

  private List<Integer> select(final Filter filter)
  {
    return Lists.newArrayList(
        Iterables.transform(
            selectUsingColumn(filter, "dim0"),
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
