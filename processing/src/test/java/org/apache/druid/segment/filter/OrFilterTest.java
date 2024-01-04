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
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class OrFilterTest extends BaseFilterTest
{
  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", DateTimes.of("2000")),
          DimensionsSpec.EMPTY
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parseBatch(ImmutableMap.of("dim0", "0", "dim1", "0")).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "1", "dim1", "0")).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "2", "dim1", "0")).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "3", "dim1", "0")).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "4", "dim1", "0")).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "5", "dim1", "0")).get(0)
  );

  public OrFilterTest(
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
    BaseFilterTest.tearDown(AndFilterTest.class.getName());
  }

  @Test
  public void testOneFilterMatchSome()
  {
    assertFilterMatches(
        new OrDimFilter(
            ImmutableList.of(
                new SelectorDimFilter("dim0", "1", null)
            )
        ),
        ImmutableList.of("1")
    );
  }

  @Test
  public void testOneFilterMatchAll()
  {
    assertFilterMatches(
        new OrDimFilter(
            ImmutableList.of(
                new SelectorDimFilter("dim1", "0", null)
            )
        ),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
  }

  @Test
  public void testOneFilterMatchNone()
  {
    assertFilterMatches(
        new OrDimFilter(
            ImmutableList.of(
                new SelectorDimFilter("dim1", "7", null)
            )
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testTwoFilterFirstMatchesAllSecondMatchesNone()
  {
    assertFilterMatches(
        new OrDimFilter(
            ImmutableList.of(
                new SelectorDimFilter("dim1", "0", null),
                new SelectorDimFilter("dim0", "7", null)
            )
        ),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
  }

  @Test
  public void testTwoFilterFirstMatchesNoneSecondMatchesAll()
  {
    assertFilterMatches(
        new OrDimFilter(
            ImmutableList.of(
                new SelectorDimFilter("dim0", "7", null),
                new SelectorDimFilter("dim1", "0", null)
            )
        ),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
  }

  @Test
  public void testTwoFilterFirstMatchesNoneSecondLiterallyTrue()
  {
    assertFilterMatches(
        new OrDimFilter(
            ImmutableList.of(
                new SelectorDimFilter("dim0", "7", null),
                TrueDimFilter.instance()
            )
        ),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
  }

  @Test
  public void testTwoFilterFirstMatchesAllSecondMatchesAll()
  {
    assertFilterMatches(
        new OrDimFilter(
            ImmutableList.of(
                new SelectorDimFilter("dim1", "0", null),
                new NotDimFilter(new SelectorDimFilter("dim0", "7", null))
            )
        ),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
  }

  @Test
  public void testTwoFilterFirstLiterallyTrueSecondMatchesAll()
  {
    assertFilterMatches(
        new OrDimFilter(
            ImmutableList.of(
                TrueDimFilter.instance(),
                new NotDimFilter(new SelectorDimFilter("dim0", "7", null))
            )
        ),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
  }

  @Test
  public void testTwoFilterFirstMatchesSomeSecondMatchesNone()
  {
    assertFilterMatches(
        new OrDimFilter(
            ImmutableList.of(
                new SelectorDimFilter("dim0", "3", null),
                new SelectorDimFilter("dim1", "7", null)
            )
        ),
        ImmutableList.of("3")
    );
  }

  @Test
  public void testTwoFilterFirstMatchesNoneSecondMatchesSome()
  {
    assertFilterMatches(
        new OrDimFilter(
            ImmutableList.of(
                new SelectorDimFilter("dim1", "7", null),
                new SelectorDimFilter("dim0", "3", null)
            )
        ),
        ImmutableList.of("3")
    );
  }

  @Test
  public void testTwoFilterFirstMatchesNoneSecondMatchesNone()
  {
    assertFilterMatches(
        new OrDimFilter(
            ImmutableList.of(
                new SelectorDimFilter("dim1", "7", null),
                new SelectorDimFilter("dim0", "7", null)
            )
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testThreeFilterFirstMatchesSomeSecondLiterallyTrueThirdMatchesNone()
  {
    assertFilterMatches(
        new AndDimFilter(
            new InDimFilter("dim0", ImmutableSet.of("0", "1", "2", "4", "5")),
            new OrDimFilter(
                ImmutableList.of(
                    new SelectorDimFilter("dim0", "4", null),
                    TrueDimFilter.instance(),
                    new SelectorDimFilter("dim0", "7", null)
                )
            )
        ),
        ImmutableList.of("0", "1", "2", "4", "5")
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(OrDimFilter.class).usingGetClass().withIgnoredFields("optimizedFilterIncludeUnknown", "optimizedFilterNoIncludeUnknown").verify();
    EqualsVerifier.forClass(OrFilter.class).usingGetClass().withNonnullFields("filters").verify();
  }
}
