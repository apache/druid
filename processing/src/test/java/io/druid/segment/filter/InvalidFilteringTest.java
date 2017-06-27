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
import io.druid.java.util.common.Pair;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.segment.IndexBuilder;
import io.druid.segment.StorageAdapter;
import io.druid.segment.incremental.IncrementalIndexSchema;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class InvalidFilteringTest extends BaseFilterTest
{
  private static final String COUNT_COLUMN = "count";
  private static final String TIMESTAMP_COLUMN = "ts";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "millis", new DateTime("2000")),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim0", "dim1", "dim2", "dim3")),
              null,
              null
          )
      )
  );

  private static final InputRow row0 = PARSER.parse(ImmutableMap.<String, Object>of("ts", 1L, "dim0", "1", "dim1", "", "dim2", ImmutableList.of("a", "b")));
  private static final InputRow row1 = PARSER.parse(ImmutableMap.<String, Object>of("ts", 2L, "dim0", "2", "dim1", "10", "dim2", ImmutableList.of()));
  private static final InputRow row2 = PARSER.parse(ImmutableMap.<String, Object>of("ts", 3L, "dim0", "3", "dim1", "2", "dim2", ImmutableList.of("")));
  private static final InputRow row3 = PARSER.parse(ImmutableMap.<String, Object>of("ts", 4L, "dim0", "4", "dim1", "1", "dim2", ImmutableList.of("a")));
  private static final InputRow row4 = PARSER.parse(ImmutableMap.<String, Object>of("ts", 5L, "dim0", "5", "dim1", "def", "dim2", ImmutableList.of("c")));
  private static final InputRow row5 = PARSER.parse(ImmutableMap.<String, Object>of("ts", 6L, "dim0", "6", "dim1", "abc"));

  private static final List<InputRow> ROWS = ImmutableList.of(
      row0,
      row1,
      row2,
      row3,
      row4,
      row5
  );

  public InvalidFilteringTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean cnf,
      boolean optimize
  )
  {
    super(testName, ROWS, overrideIndexBuilderSchema(indexBuilder), finisher, cnf, optimize);
  }

  private static IndexBuilder overrideIndexBuilderSchema(IndexBuilder indexBuilder)
  {
    IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMetrics(
            new CountAggregatorFactory("count"),
            new HyperUniquesAggregatorFactory("hyperion", "dim1"),
            new DoubleMaxAggregatorFactory("dmax", "dim0")
        ).build();

    return indexBuilder.schema(schema);
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    BaseFilterTest.tearDown(InvalidFilteringTest.class.getName());
  }

  @Test
  public void testFilterTheUnfilterable()
  {
    // single value matching
    assertFilterMatches(
        new SelectorDimFilter("hyperion", "a string", null),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new SelectorDimFilter("hyperion", null, null),
        ImmutableList.<String>of("1", "2", "3", "4", "5", "6")
    );

    // predicate based matching
    assertFilterMatches(
        new InDimFilter("hyperion", Arrays.asList("hello", "world"), null),
        ImmutableList.<String>of()
    );

    assertFilterMatches(
        new InDimFilter("hyperion", Arrays.asList("hello", "world", null), null),
        ImmutableList.<String>of("1", "2", "3", "4", "5", "6")
    );
  }
}
