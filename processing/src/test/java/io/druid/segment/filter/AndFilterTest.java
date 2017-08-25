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
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.SelectorDimFilter;
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
public class AndFilterTest extends BaseFilterTest
{
  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", DateTimes.of("2000")),
          new DimensionsSpec(null, null, null)
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "0", "dim1", "0")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "1", "dim1", "0")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "2", "dim1", "0")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "3", "dim1", "0")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "4", "dim1", "0")),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "5", "dim1", "0"))
  );

  public AndFilterTest(
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
  public void testAnd()
  {
    assertFilterMatches(
        new AndDimFilter(ImmutableList.<DimFilter>of(
            new SelectorDimFilter("dim0", "0", null),
            new SelectorDimFilter("dim1", "0", null)
        )),
        ImmutableList.of("0")
    );
    assertFilterMatches(
        new AndDimFilter(ImmutableList.<DimFilter>of(
            new SelectorDimFilter("dim0", "0", null),
            new SelectorDimFilter("dim1", "1", null)
        )),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new AndDimFilter(ImmutableList.<DimFilter>of(
            new SelectorDimFilter("dim0", "1", null),
            new SelectorDimFilter("dim1", "0", null)
        )),
        ImmutableList.of("1")
    );
    assertFilterMatches(
        new AndDimFilter(ImmutableList.<DimFilter>of(
            new SelectorDimFilter("dim0", "1", null),
            new SelectorDimFilter("dim1", "1", null)
        )),
        ImmutableList.<String>of()
    );
    assertFilterMatches(
        new AndDimFilter(ImmutableList.<DimFilter>of(
            new NotDimFilter(new SelectorDimFilter("dim0", "1", null)),
            new NotDimFilter(new SelectorDimFilter("dim1", "1", null))
        )),
        ImmutableList.of("0", "2", "3", "4", "5")
    );
    assertFilterMatches(
        new AndDimFilter(ImmutableList.<DimFilter>of(
            new NotDimFilter(new SelectorDimFilter("dim0", "0", null)),
            new NotDimFilter(new SelectorDimFilter("dim1", "0", null))
        )),
        ImmutableList.<String>of()
    );
  }

  @Test
  public void testNotAnd()
  {
    assertFilterMatches(
        new NotDimFilter(new AndDimFilter(ImmutableList.<DimFilter>of(
            new SelectorDimFilter("dim0", "0", null),
            new SelectorDimFilter("dim1", "0", null)
        ))),
        ImmutableList.of("1", "2", "3", "4", "5")
    );
    assertFilterMatches(
        new NotDimFilter(new AndDimFilter(ImmutableList.<DimFilter>of(
            new SelectorDimFilter("dim0", "0", null),
            new SelectorDimFilter("dim1", "1", null)
        ))),
        ImmutableList.<String>of("0", "1", "2", "3", "4", "5")
    );
    assertFilterMatches(
        new NotDimFilter(new AndDimFilter(ImmutableList.<DimFilter>of(
            new SelectorDimFilter("dim0", "1", null),
            new SelectorDimFilter("dim1", "0", null)
        ))),
        ImmutableList.of("0", "2", "3", "4", "5")
    );
    assertFilterMatches(
        new NotDimFilter(new AndDimFilter(ImmutableList.<DimFilter>of(
            new SelectorDimFilter("dim0", "1", null),
            new SelectorDimFilter("dim1", "1", null)
        ))),
        ImmutableList.<String>of("0", "1", "2", "3", "4", "5")
    );
    assertFilterMatches(
        new NotDimFilter(new AndDimFilter(ImmutableList.<DimFilter>of(
            new NotDimFilter(new SelectorDimFilter("dim0", "1", null)),
            new NotDimFilter(new SelectorDimFilter("dim1", "1", null))
        ))),
        ImmutableList.of("1")
    );
    assertFilterMatches(
        new NotDimFilter(new AndDimFilter(ImmutableList.<DimFilter>of(
            new NotDimFilter(new SelectorDimFilter("dim0", "0", null)),
            new NotDimFilter(new SelectorDimFilter("dim1", "0", null))
        ))),
        ImmutableList.<String>of("0", "1", "2", "3", "4", "5")
    );
  }
}
