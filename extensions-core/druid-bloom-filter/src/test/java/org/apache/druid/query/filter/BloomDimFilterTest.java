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

package org.apache.druid.query.filter;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.BloomFilterSerializersModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.extraction.TimeDimExtractionFn;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.filter.BaseFilterTest;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.hive.common.util.BloomKFilter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class BloomDimFilterTest extends BaseFilterTest
{
  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", DateTimes.of("2000")),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim0", "dim1", "dim2", "dim3", "dim6")),
              null,
              null
          )
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parseBatch(ImmutableMap.of(
          "dim0",
          "0",
          "dim1",
          "",
          "dim2",
          ImmutableList.of("a", "b"),
          "dim6",
          "2017-07-25"
      )).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "1", "dim1", "10", "dim2", ImmutableList.of(), "dim6", "2017-07-25"))
            .get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "2", "dim1", "2", "dim2", ImmutableList.of(""), "dim6", "2017-05-25"))
            .get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "3", "dim1", "1", "dim2", ImmutableList.of("a"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "4", "dim1", "def", "dim2", ImmutableList.of("c"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "5", "dim1", "abc")).get(0)
  );

  public BloomDimFilterTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean cnf,
      boolean optimize
  )
  {
    super(
        testName,
        ROWS,
        indexBuilder.schema(
            new IncrementalIndexSchema.Builder()
                .withDimensionsSpec(PARSER.getParseSpec().getDimensionsSpec()).build()
        ),
        finisher,
        cnf,
        optimize
    );
  }

  private static DefaultObjectMapper mapper = new DefaultObjectMapper();

  @BeforeClass
  public static void beforeClass()
  {
    mapper.registerModule(new BloomFilterSerializersModule());
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    BaseFilterTest.tearDown(BloomDimFilterTest.class.getName());
  }

  @Test
  public void testSerde() throws IOException
  {
    BloomKFilter bloomFilter = new BloomKFilter(1500);
    bloomFilter.addString("myTestString");
    BloomDimFilter bloomDimFilter = new BloomDimFilter(
        "abc",
        bloomFilter,
        new TimeDimExtractionFn("yyyy-MM-dd", "yyyy-MM", true)
    );
    DimFilter filter = mapper.readValue(mapper.writeValueAsBytes(bloomDimFilter), DimFilter.class);
    Assert.assertTrue(filter instanceof BloomDimFilter);
    BloomDimFilter serde = (BloomDimFilter) filter;
    Assert.assertEquals(bloomDimFilter.getDimension(), serde.getDimension());
    Assert.assertEquals(bloomDimFilter.getExtractionFn(), serde.getExtractionFn());
    Assert.assertTrue(bloomDimFilter.getBloomKFilter().testString("myTestString"));
    Assert.assertFalse(bloomDimFilter.getBloomKFilter().testString("not_match"));
  }

  @Test
  public void testWithTimeExtractionFnNull()
  {
    assertFilterMatches(new BloomDimFilter(
        "dim0",
        bloomKFilter(1000, null, ""),
        new TimeDimExtractionFn("yyyy-MM-dd", "yyyy-MM", true)
    ), ImmutableList.of());
    assertFilterMatches(new BloomDimFilter(
        "dim6",
        bloomKFilter(1000, null, ""),
        new TimeDimExtractionFn("yyyy-MM-dd", "yyyy-MM", true)
    ), ImmutableList.of("3", "4", "5"));
    assertFilterMatches(new BloomDimFilter(
        "dim6",
        bloomKFilter(1000, "2017-07"),
        new TimeDimExtractionFn("yyyy-MM-dd", "yyyy-MM", true)
    ), ImmutableList.of("0", "1"));
    assertFilterMatches(new BloomDimFilter(
        "dim6",
        bloomKFilter(1000, "2017-05"),
        new TimeDimExtractionFn("yyyy-MM-dd", "yyyy-MM", true)
    ), ImmutableList.of("2"));
  }

  @Test
  public void testSingleValueStringColumnWithoutNulls()
  {
    assertFilterMatches(new BloomDimFilter("dim0", bloomKFilter(1000, (String) null), null), ImmutableList.of());
    assertFilterMatches(new BloomDimFilter("dim0", bloomKFilter(1000, ""), null), ImmutableList.of());
    assertFilterMatches(new BloomDimFilter("dim0", bloomKFilter(1000, "0"), null), ImmutableList.of("0"));
    assertFilterMatches(new BloomDimFilter("dim0", bloomKFilter(1000, "1"), null), ImmutableList.of("1"));
  }

  @Test
  public void testSingleValueStringColumnWithNulls()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(new BloomDimFilter("dim1", bloomKFilter(1000, (String) null), null), ImmutableList.of("0"));
    } else {
      assertFilterMatches(new BloomDimFilter("dim1", bloomKFilter(1000, (String) null), null), ImmutableList.of());
      assertFilterMatches(new BloomDimFilter("dim1", bloomKFilter(1000, ""), null), ImmutableList.of("0"));
    }
    assertFilterMatches(new BloomDimFilter("dim1", bloomKFilter(1000, "10"), null), ImmutableList.of("1"));
    assertFilterMatches(new BloomDimFilter("dim1", bloomKFilter(1000, "2"), null), ImmutableList.of("2"));
    assertFilterMatches(new BloomDimFilter("dim1", bloomKFilter(1000, "1"), null), ImmutableList.of("3"));
    assertFilterMatches(new BloomDimFilter("dim1", bloomKFilter(1000, "def"), null), ImmutableList.of("4"));
    assertFilterMatches(new BloomDimFilter("dim1", bloomKFilter(1000, "abc"), null), ImmutableList.of("5"));
    assertFilterMatches(new BloomDimFilter("dim1", bloomKFilter(1000, "ab"), null), ImmutableList.of());
  }

  @Test
  public void testMultiValueStringColumn()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          new BloomDimFilter("dim2", bloomKFilter(1000, (String) null), null),
          ImmutableList.of("1", "2", "5")
      );
    } else {
      assertFilterMatches(
          new BloomDimFilter("dim2", bloomKFilter(1000, (String) null), null),
          ImmutableList.of("1", "5")
      );
      assertFilterMatches(new BloomDimFilter("dim2", bloomKFilter(1000, ""), null), ImmutableList.of("2"));
    }
    assertFilterMatches(new BloomDimFilter("dim2", bloomKFilter(1000, "a"), null), ImmutableList.of("0", "3"));
    assertFilterMatches(new BloomDimFilter("dim2", bloomKFilter(1000, "b"), null), ImmutableList.of("0"));
    assertFilterMatches(new BloomDimFilter("dim2", bloomKFilter(1000, "c"), null), ImmutableList.of("4"));
    assertFilterMatches(new BloomDimFilter("dim2", bloomKFilter(1000, "d"), null), ImmutableList.of());
  }

  @Test
  public void testMissingColumnSpecifiedInDimensionList()
  {
    assertFilterMatches(
        new BloomDimFilter("dim3", bloomKFilter(1000, (String) null), null),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
    assertFilterMatches(new BloomDimFilter("dim3", bloomKFilter(1000, ""), null), ImmutableList.of());
    assertFilterMatches(new BloomDimFilter("dim3", bloomKFilter(1000, "a"), null), ImmutableList.of());
    assertFilterMatches(new BloomDimFilter("dim3", bloomKFilter(1000, "b"), null), ImmutableList.of());
    assertFilterMatches(new BloomDimFilter("dim3", bloomKFilter(1000, "c"), null), ImmutableList.of());
  }

  @Test
  public void testMissingColumnNotSpecifiedInDimensionList()
  {
    assertFilterMatches(
        new BloomDimFilter("dim4", bloomKFilter(1000, (String) null), null),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
    assertFilterMatches(new BloomDimFilter("dim4", bloomKFilter(1000, ""), null), ImmutableList.of());
    assertFilterMatches(new BloomDimFilter("dim4", bloomKFilter(1000, "a"), null), ImmutableList.of());
    assertFilterMatches(new BloomDimFilter("dim4", bloomKFilter(1000, "b"), null), ImmutableList.of());
    assertFilterMatches(new BloomDimFilter("dim4", bloomKFilter(1000, "c"), null), ImmutableList.of());
  }

  @Test
  public void testExpressionVirtualColumn()
  {
    assertFilterMatches(
        new BloomDimFilter("expr", bloomKFilter(1000, 1.1F), null),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
    assertFilterMatches(new BloomDimFilter("expr", bloomKFilter(1000, 1.2F), null), ImmutableList.of());
    assertFilterMatches(
        new BloomDimFilter("exprDouble", bloomKFilter(1000, 2.1D), null),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
    assertFilterMatches(new BloomDimFilter("exprDouble", bloomKFilter(1000, 2.2D), null), ImmutableList.of());
    assertFilterMatches(
        new BloomDimFilter("exprLong", bloomKFilter(1000, 3L), null),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
    assertFilterMatches(new BloomDimFilter("exprLong", bloomKFilter(1000, 4L), null), ImmutableList.of());
  }

  @Test
  public void testSelectorWithLookupExtractionFn()
  {
    final Map<String, String> stringMap = ImmutableMap.of(
        "1", "HELLO",
        "a", "HELLO",
        "def", "HELLO",
        "abc", "UNKNOWN"
    );
    LookupExtractor mapExtractor = new MapLookupExtractor(stringMap, false);
    LookupExtractionFn lookupFn = new LookupExtractionFn(mapExtractor, false, "UNKNOWN", false, true);

    assertFilterMatches(new BloomDimFilter("dim0", bloomKFilter(1000, "HELLO"), lookupFn), ImmutableList.of("1"));
    assertFilterMatches(
        new BloomDimFilter("dim0", bloomKFilter(1000, "UNKNOWN"), lookupFn),
        ImmutableList.of("0", "2", "3", "4", "5")
    );

    assertFilterMatches(new BloomDimFilter("dim1", bloomKFilter(1000, "HELLO"), lookupFn), ImmutableList.of("3", "4"));
    assertFilterMatches(
        new BloomDimFilter("dim1", bloomKFilter(1000, "UNKNOWN"), lookupFn),
        ImmutableList.of("0", "1", "2", "5")
    );

    assertFilterMatches(new BloomDimFilter("dim2", bloomKFilter(1000, "HELLO"), lookupFn), ImmutableList.of("0", "3"));
    assertFilterMatches(
        new BloomDimFilter("dim2", bloomKFilter(1000, "UNKNOWN"), lookupFn),
        ImmutableList.of("0", "1", "2", "4", "5")
    );

    assertFilterMatches(new BloomDimFilter("dim3", bloomKFilter(1000, "HELLO"), lookupFn), ImmutableList.of());
    assertFilterMatches(
        new BloomDimFilter("dim3", bloomKFilter(1000, "UNKNOWN"), lookupFn),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );

    assertFilterMatches(new BloomDimFilter("dim4", bloomKFilter(1000, "HELLO"), lookupFn), ImmutableList.of());
    assertFilterMatches(
        new BloomDimFilter("dim4", bloomKFilter(1000, "UNKNOWN"), lookupFn),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );

    final Map<String, String> stringMap2 = ImmutableMap.of(
        "2", "5"
    );
    LookupExtractor mapExtractor2 = new MapLookupExtractor(stringMap2, false);
    LookupExtractionFn lookupFn2 = new LookupExtractionFn(mapExtractor2, true, null, false, true);
    assertFilterMatches(new BloomDimFilter("dim0", bloomKFilter(1000, "5"), lookupFn2), ImmutableList.of("2", "5"));

    final Map<String, String> stringMap3 = ImmutableMap.of(
        "1", ""
    );
    LookupExtractor mapExtractor3 = new MapLookupExtractor(stringMap3, false);
    LookupExtractionFn lookupFn3 = new LookupExtractionFn(mapExtractor3, false, null, false, true);
    if (NullHandling.replaceWithDefault()) {
      // Nulls and empty strings are considered equivalent
      assertFilterMatches(
          new BloomDimFilter("dim0", bloomKFilter(1000, (String) null), lookupFn3),
          ImmutableList.of("0", "1", "2", "3", "4", "5")
      );
    } else {
      assertFilterMatches(
          new BloomDimFilter("dim0", bloomKFilter(1000, (String) null), lookupFn3),
          ImmutableList.of("0", "2", "3", "4", "5")
      );
      assertFilterMatches(
          new BloomDimFilter("dim0", bloomKFilter(1000, ""), lookupFn3),
          ImmutableList.of("1")
      );
    }
  }

  private static BloomKFilter bloomKFilter(int expectedEntries, String... values)
  {
    BloomKFilter filter = new BloomKFilter(expectedEntries);
    for (String value : values) {
      if (value == null) {
        filter.addBytes(null, 0, 0);
      } else {
        filter.addString(value);
      }
    }
    return filter;
  }

  private static BloomKFilter bloomKFilter(int expectedEntries, Float... values)
  {
    BloomKFilter filter = new BloomKFilter(expectedEntries);
    for (Float value : values) {
      if (value == null) {
        filter.addBytes(null, 0, 0);
      } else {
        filter.addFloat(value);
      }
    }
    return filter;
  }

  private static BloomKFilter bloomKFilter(int expectedEntries, Double... values)
  {
    BloomKFilter filter = new BloomKFilter(expectedEntries);
    for (Double value : values) {
      if (value == null) {
        filter.addBytes(null, 0, 0);
      } else {
        filter.addDouble(value);
      }
    }
    return filter;
  }

  private static BloomKFilter bloomKFilter(int expectedEntries, Long... values)
  {
    BloomKFilter filter = new BloomKFilter(expectedEntries);
    for (Long value : values) {
      if (value == null) {
        filter.addBytes(null, 0, 0);
      } else {
        filter.addLong(value);
      }
    }
    return filter;
  }
}
