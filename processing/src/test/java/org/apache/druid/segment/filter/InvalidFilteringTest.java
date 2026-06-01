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
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
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
  private static final String TIMESTAMP_COLUMN = "ts";
  
  private static final InputRowSchema SCHEMA = new InputRowSchema(
      new TimestampSpec(TIMESTAMP_COLUMN, "millis", DateTimes.of("2000")),
      new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("dim0", "dim1", "dim2", "dim3"))),
      ColumnsFilter.all()
  );

  private static final InputRow ROW0 = makeMapRow(SCHEMA, Map.of("ts", 1L, "dim0", "1", "dim1", "", "dim2", List.of("a", "b")));
  private static final InputRow ROW1 = makeMapRow(SCHEMA, Map.of("ts", 2L, "dim0", "2", "dim1", "10", "dim2", List.of()));
  private static final InputRow ROW2 = makeMapRow(SCHEMA, Map.of("ts", 3L, "dim0", "3", "dim1", "2", "dim2", List.of("")));
  private static final InputRow ROW3 = makeMapRow(SCHEMA, Map.of("ts", 4L, "dim0", "4", "dim1", "1", "dim2", List.of("a")));
  private static final InputRow ROW4 = makeMapRow(SCHEMA, Map.of("ts", 5L, "dim0", "5", "dim1", "def", "dim2", List.of("c")));
  private static final InputRow ROW5 = makeMapRow(SCHEMA, Map.of("ts", 6L, "dim0", "6", "dim1", "abc"));

  private static final List<InputRow> ROWS = List.of(
      ROW0,
      ROW1,
      ROW2,
      ROW3,
      ROW4,
      ROW5
  );

  public InvalidFilteringTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<CursorFactory, Closeable>> finisher,
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
        List.of()
    );

    assertFilterMatches(
        new SelectorDimFilter("hyperion", null, null),
        List.of("1", "2", "3", "4", "5", "6")
    );

    // predicate based matching
    assertFilterMatches(
        new InDimFilter("hyperion", Arrays.asList("hello", "world"), null),
        List.of()
    );

    assertFilterMatches(
        new InDimFilter("hyperion", Arrays.asList("hello", "world", null), null),
        List.of("1", "2", "3", "4", "5", "6")
    );
  }
}
