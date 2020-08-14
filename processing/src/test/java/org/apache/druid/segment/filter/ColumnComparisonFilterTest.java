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
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.filter.ColumnComparisonDimFilter;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class ColumnComparisonFilterTest extends BaseFilterTest
{
  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", DateTimes.of("2000")),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim0", "dim1", "dim2")),
              null,
              null
          )
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parseBatch(ImmutableMap.of("dim0", "0", "dim1", "", "dim2", ImmutableList.of("1", "2"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "1", "dim1", "10", "dim2", ImmutableList.of())).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "2", "dim1", "2", "dim2", ImmutableList.of(""))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "3", "dim1", "1", "dim2", ImmutableList.of("3"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "4", "dim1", "1", "dim2", ImmutableList.of("4", "5"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "5", "dim1", "5", "dim2", ImmutableList.of("4", "5"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "6", "dim1", "1")).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "7", "dim1", "a")).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "8", "dim1", 8L)).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "9", "dim1", 1.234f, "dim2", 1.234f)).get(0)
  );

  public ColumnComparisonFilterTest(
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
    BaseFilterTest.tearDown(ColumnComparisonFilterTest.class.getName());
  }

  @Test
  public void testColumnsWithoutNulls()
  {
    assertFilterMatchesSkipVectorize(new ColumnComparisonDimFilter(ImmutableList.of(
        DefaultDimensionSpec.of("dim0"),
        DefaultDimensionSpec.of("dim1")
    )), ImmutableList.of("2", "5", "8"));
    assertFilterMatchesSkipVectorize(new ColumnComparisonDimFilter(ImmutableList.of(
        DefaultDimensionSpec.of("dim0"),
        DefaultDimensionSpec.of("dim2")
    )), ImmutableList.of("3", "4", "5"));
    assertFilterMatchesSkipVectorize(new ColumnComparisonDimFilter(ImmutableList.of(
        DefaultDimensionSpec.of("dim1"),
        DefaultDimensionSpec.of("dim2")
    )), ImmutableList.of("5", "9"));
    assertFilterMatchesSkipVectorize(new ColumnComparisonDimFilter(ImmutableList.of(
        DefaultDimensionSpec.of("dim0"),
        DefaultDimensionSpec.of("dim1"),
        DefaultDimensionSpec.of("dim2")
    )), ImmutableList.of("5"));
  }

  @Test
  public void testMissingColumnNotSpecifiedInDimensionList()
  {
    assertFilterMatchesSkipVectorize(new ColumnComparisonDimFilter(ImmutableList.of(
        DefaultDimensionSpec.of("dim6"),
        DefaultDimensionSpec.of("dim7")
    )), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));

    if (NullHandling.replaceWithDefault()) {
      // "" is equivalent to null which is equivalent to a missing dimension
      assertFilterMatchesSkipVectorize(new ColumnComparisonDimFilter(ImmutableList.of(
          DefaultDimensionSpec.of("dim1"),
          DefaultDimensionSpec.of("dim6")
      )), ImmutableList.of("0"));

      assertFilterMatchesSkipVectorize(new ColumnComparisonDimFilter(ImmutableList.of(
          DefaultDimensionSpec.of("dim2"),
          DefaultDimensionSpec.of("dim6")
      )), ImmutableList.of("1", "2", "6", "7", "8"));

      assertFilterMatchesSkipVectorize(
          new ColumnComparisonDimFilter(
              ImmutableList.of(DefaultDimensionSpec.of("dim1"), DefaultDimensionSpec.of("dim6"))
          ),
          ImmutableList.of("0")
      );

      assertFilterMatchesSkipVectorize(
          new ColumnComparisonDimFilter(
              ImmutableList.of(DefaultDimensionSpec.of("dim2"), DefaultDimensionSpec.of("dim6"))
          ),
          ImmutableList.of("1", "2", "6", "7", "8")
      );
    } else {
      // "" is not equivalent to a missing dimension
      assertFilterMatchesSkipVectorize(new ColumnComparisonDimFilter(ImmutableList.of(
          DefaultDimensionSpec.of("dim1"),
          DefaultDimensionSpec.of("dim6")
      )), Collections.emptyList());

      assertFilterMatchesSkipVectorize(new ColumnComparisonDimFilter(ImmutableList.of(
          DefaultDimensionSpec.of("dim2"),
          DefaultDimensionSpec.of("dim6")
      )), ImmutableList.of("1", "6", "7", "8"));

      assertFilterMatchesSkipVectorize(
          new ColumnComparisonDimFilter(
              ImmutableList.of(DefaultDimensionSpec.of("dim1"), DefaultDimensionSpec.of("dim6"))
          ),
          ImmutableList.of()
      );

      assertFilterMatchesSkipVectorize(
          new ColumnComparisonDimFilter(
              ImmutableList.of(DefaultDimensionSpec.of("dim2"), DefaultDimensionSpec.of("dim6"))
          ),
          ImmutableList.of("1", "6", "7", "8")
      );
    }
  }

  @Test
  public void testSelectorWithLookupExtractionFn()
  {
    final Map<String, String> stringMap = ImmutableMap.of(
        "a", "7"
    );
    LookupExtractor mapExtractor = new MapLookupExtractor(stringMap, false);
    LookupExtractionFn lookupFn = new LookupExtractionFn(mapExtractor, true, null, false, true);

    assertFilterMatchesSkipVectorize(new ColumnComparisonDimFilter(ImmutableList.of(
        new ExtractionDimensionSpec("dim0", "dim0", lookupFn),
        new ExtractionDimensionSpec("dim1", "dim1", lookupFn)
    )), ImmutableList.of("2", "5", "7", "8"));
  }

  @Test
  public void testEqualsContract()
  {
    EqualsVerifier.forClass(ColumnComparisonFilter.class)
                  .usingGetClass()
                  .verify();
  }
}
