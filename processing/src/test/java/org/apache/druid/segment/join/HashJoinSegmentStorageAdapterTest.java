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

package org.apache.druid.segment.join;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.join.lookup.LookupJoinable;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.timeline.SegmentId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;

public class HashJoinSegmentStorageAdapterTest
{
  private static final String FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX = "c1.";
  private static final String FACT_TO_COUNTRY_ON_NUMBER_PREFIX = "c2.";
  private static final String FACT_TO_REGION_PREFIX = "r1.";
  private static final String REGION_TO_COUNTRY_PREFIX = "rtc.";
  private static Long NULL_COUNTRY;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public QueryableIndexSegment factSegment;
  public LookupExtractor countryIsoCodeToNameLookup;
  public LookupExtractor countryNumberToNameLookup;
  public IndexedTable countriesTable;
  public IndexedTable regionsTable;

  @BeforeClass
  public static void setUpStatic()
  {
    NullHandling.initializeForTests();
    NULL_COUNTRY = NullHandling.sqlCompatible() ? null : 0L;
  }

  @Before
  public void setUp() throws IOException
  {
    factSegment = new QueryableIndexSegment(
        JoinTestHelper.createFactIndexBuilder(temporaryFolder.newFolder()).buildMMappedIndex(),
        SegmentId.dummy("facts")
    );
    countryIsoCodeToNameLookup = JoinTestHelper.createCountryIsoCodeToNameLookup();
    countryNumberToNameLookup = JoinTestHelper.createCountryNumberToNameLookup();
    countriesTable = JoinTestHelper.createCountriesIndexedTable();
    regionsTable = JoinTestHelper.createRegionsIndexedTable();
  }

  @After
  public void tearDown()
  {
    if (factSegment != null) {
      factSegment.close();
    }
  }

  @Test
  public void test_getInterval_factToCountry()
  {
    Assert.assertEquals(
        Intervals.of("2015-09-12/2015-09-12T02:33:40.060Z"),
        makeFactToCountrySegment().getInterval()
    );
  }

  @Test
  public void test_getAvailableDimensions_factToCountry()
  {
    Assert.assertEquals(
        ImmutableList.of(
            "channel",
            "regionIsoCode",
            "countryNumber",
            "countryIsoCode",
            "user",
            "isRobot",
            "isAnonymous",
            "namespace",
            "page",
            "delta",
            "c1.countryIsoCode",
            "c1.countryName",
            "c1.countryNumber"
        ),
        Lists.newArrayList(makeFactToCountrySegment().getAvailableDimensions().iterator())
    );
  }

  @Test
  public void test_getAvailableMetrics_factToCountry()
  {
    Assert.assertEquals(
        ImmutableList.of("channel_uniques"),
        Lists.newArrayList(makeFactToCountrySegment().getAvailableMetrics().iterator())
    );
  }

  @Test
  public void test_getDimensionCardinality_factToCountryFactColumn()
  {
    Assert.assertEquals(
        15,
        makeFactToCountrySegment().getDimensionCardinality("countryIsoCode")
    );
  }

  @Test
  public void test_getDimensionCardinality_factToCountryJoinColumn()
  {
    Assert.assertEquals(
        15,
        makeFactToCountrySegment().getDimensionCardinality(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName")
    );
  }

  @Test
  public void test_getDimensionCardinality_factToCountryNonexistentFactColumn()
  {
    Assert.assertEquals(
        1,
        makeFactToCountrySegment().getDimensionCardinality("nonexistent")
    );
  }

  @Test
  public void test_getDimensionCardinality_factToCountryNonexistentJoinColumn()
  {
    Assert.assertEquals(
        1,
        makeFactToCountrySegment().getDimensionCardinality(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "nonexistent")
    );
  }

  @Test
  public void test_getMinTime_factToCountry()
  {
    Assert.assertEquals(
        DateTimes.of("2015-09-12T00:46:58.771Z"),
        makeFactToCountrySegment().getMinTime()
    );
  }

  @Test
  public void test_getMaxTime_factToCountry()
  {
    Assert.assertEquals(
        DateTimes.of("2015-09-12T02:33:40.059Z"),
        makeFactToCountrySegment().getMaxTime()
    );
  }

  @Test
  public void test_getMinValue_factToCountryFactColumn()
  {
    Assert.assertNull(makeFactToCountrySegment().getMinValue("countryIsoCode"));
  }

  @Test
  public void test_getMinValue_factToCountryJoinColumn()
  {
    Assert.assertNull(makeFactToCountrySegment().getMinValue(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryIsoCode"));
  }

  @Test
  public void test_getMinValue_factToCountryNonexistentFactColumn()
  {
    Assert.assertNull(makeFactToCountrySegment().getMinValue("nonexistent"));
  }

  @Test
  public void test_getMinValue_factToCountryNonexistentJoinColumn()
  {
    Assert.assertNull(makeFactToCountrySegment().getMinValue(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "nonexistent"));
  }

  @Test
  public void test_getMaxValue_factToCountryFactColumn()
  {
    Assert.assertEquals(
        "US",
        makeFactToCountrySegment().getMaxValue("countryIsoCode")
    );
  }

  @Test
  public void test_getMaxValue_factToCountryJoinColumn()
  {
    Assert.assertNull(makeFactToCountrySegment().getMaxValue(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName"));
  }

  @Test
  public void test_getMaxValue_factToCountryNonexistentFactColumn()
  {
    Assert.assertNull(makeFactToCountrySegment().getMaxValue("nonexistent"));
  }

  @Test
  public void test_getMaxValue_factToCountryNonexistentJoinColumn()
  {
    Assert.assertNull(makeFactToCountrySegment().getMaxValue(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "nonexistent"));
  }

  @Test
  public void test_getCapabilities_factToCountry()
  {
    Assert.assertFalse(makeFactToCountrySegment().getCapabilities().dimensionValuesSorted());
  }

  @Test
  public void test_getColumnCapabilities_factToCountryFactColumn()
  {
    final ColumnCapabilities capabilities = makeFactToCountrySegment().getColumnCapabilities("countryIsoCode");

    Assert.assertEquals(ValueType.STRING, capabilities.getType());
    Assert.assertTrue(capabilities.hasBitmapIndexes());
    Assert.assertTrue(capabilities.isDictionaryEncoded());
  }

  @Test
  public void test_getColumnCapabilities_factToCountryJoinColumn()
  {
    final ColumnCapabilities capabilities = makeFactToCountrySegment().getColumnCapabilities(
        FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryIsoCode"
    );

    Assert.assertEquals(ValueType.STRING, capabilities.getType());
    Assert.assertFalse(capabilities.hasBitmapIndexes());
    Assert.assertTrue(capabilities.isDictionaryEncoded());
  }

  @Test
  public void test_getColumnCapabilities_factToCountryNonexistentFactColumn()
  {
    final ColumnCapabilities capabilities = makeFactToCountrySegment()
        .getColumnCapabilities("nonexistent");

    Assert.assertNull(capabilities);
  }

  @Test
  public void test_getColumnCapabilities_factToCountryNonexistentJoinColumn()
  {
    final ColumnCapabilities capabilities = makeFactToCountrySegment()
        .getColumnCapabilities(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "nonexistent");

    Assert.assertNull(capabilities);
  }

  @Test
  public void test_getColumnTypeName_factToCountryFactColumn()
  {
    Assert.assertEquals("hyperUnique", makeFactToCountrySegment().getColumnTypeName("channel_uniques"));
  }

  @Test
  public void test_getColumnTypeName_factToCountryJoinColumn()
  {
    Assert.assertEquals(
        "STRING",
        makeFactToCountrySegment().getColumnTypeName(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName")
    );
  }

  @Test
  public void test_getColumnTypeName_factToCountryNonexistentFactColumn()
  {
    Assert.assertNull(makeFactToCountrySegment().getColumnTypeName("nonexistent"));
  }

  @Test
  public void test_getColumnTypeName_factToCountryNonexistentJoinColumn()
  {
    Assert.assertNull(
        makeFactToCountrySegment().getColumnTypeName(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "nonexistent")
    );
  }

  @Test
  public void test_getMaxIngestedEventTime_factToCountry()
  {
    Assert.assertEquals(
        DateTimes.of("2015-09-12T02:33:40.059Z"),
        makeFactToCountrySegment().getMaxIngestedEventTime()
    );
  }

  @Test
  public void test_getNumRows_factToCountry()
  {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Cannot retrieve number of rows from join segment");

    makeFactToCountrySegment().getNumRows();
  }

  @Test
  public void test_getMetadata_factToCountry()
  {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Cannot retrieve metadata from join segment");

    makeFactToCountrySegment().getMetadata();
  }

  @Test
  public void test_makeCursors_factToCountryLeft()
  {
    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(factToCountryOnIsoCode(JoinType.LEFT))
        ).makeCursors(
            null,
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryNumber"
        ),
        ImmutableList.of(
            new Object[]{"Talk:Oswald Tilghman", null, null, null, NULL_COUNTRY},
            new Object[]{"Rallicula", null, null, null, NULL_COUNTRY},
            new Object[]{"Peremptory norm", "AU", "AU", "Australia", 0L},
            new Object[]{"Apamea abruzzorum", null, null, null, NULL_COUNTRY},
            new Object[]{"Atractus flammigerus", null, null, null, NULL_COUNTRY},
            new Object[]{"Agama mossambica", null, null, null, NULL_COUNTRY},
            new Object[]{"Mathis Bolly", "MX", "MX", "Mexico", 10L},
            new Object[]{"유희왕 GX", "KR", "KR", "Republic of Korea", 9L},
            new Object[]{"青野武", "JP", "JP", "Japan", 8L},
            new Object[]{"Golpe de Estado en Chile de 1973", "CL", "CL", "Chile", 2L},
            new Object[]{"President of India", "US", "US", "United States", 13L},
            new Object[]{"Diskussion:Sebastian Schulz", "DE", "DE", "Germany", 3L},
            new Object[]{"Saison 9 de Secret Story", "FR", "FR", "France", 5L},
            new Object[]{"Glasgow", "GB", "GB", "United Kingdom", 6L},
            new Object[]{"Didier Leclair", "CA", "CA", "Canada", 1L},
            new Object[]{"Les Argonautes", "CA", "CA", "Canada", 1L},
            new Object[]{"Otjiwarongo Airport", "US", "US", "United States", 13L},
            new Object[]{"Sarah Michelle Gellar", "CA", "CA", "Canada", 1L},
            new Object[]{"DirecTV", "US", "US", "United States", 13L},
            new Object[]{"Carlo Curti", "US", "US", "United States", 13L},
            new Object[]{"Giusy Ferreri discography", "IT", "IT", "Italy", 7L},
            new Object[]{"Roma-Bangkok", "IT", "IT", "Italy", 7L},
            new Object[]{"Wendigo", "SV", "SV", "El Salvador", 12L},
            new Object[]{"Алиса в Зазеркалье", "NO", "NO", "Norway", 11L},
            new Object[]{"Gabinete Ministerial de Rafael Correa", "EC", "EC", "Ecuador", 4L},
            new Object[]{"Old Anatolian Turkish", "US", "US", "United States", 13L}
        )
    );
  }

  @Test
  public void test_makeCursors_factToCountryInner()
  {
    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(factToCountryOnIsoCode(JoinType.INNER))
        ).makeCursors(
            null,
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryNumber"
        ),
        ImmutableList.of(
            new Object[]{"Peremptory norm", "AU", "AU", "Australia", 0L},
            new Object[]{"Mathis Bolly", "MX", "MX", "Mexico", 10L},
            new Object[]{"유희왕 GX", "KR", "KR", "Republic of Korea", 9L},
            new Object[]{"青野武", "JP", "JP", "Japan", 8L},
            new Object[]{"Golpe de Estado en Chile de 1973", "CL", "CL", "Chile", 2L},
            new Object[]{"President of India", "US", "US", "United States", 13L},
            new Object[]{"Diskussion:Sebastian Schulz", "DE", "DE", "Germany", 3L},
            new Object[]{"Saison 9 de Secret Story", "FR", "FR", "France", 5L},
            new Object[]{"Glasgow", "GB", "GB", "United Kingdom", 6L},
            new Object[]{"Didier Leclair", "CA", "CA", "Canada", 1L},
            new Object[]{"Les Argonautes", "CA", "CA", "Canada", 1L},
            new Object[]{"Otjiwarongo Airport", "US", "US", "United States", 13L},
            new Object[]{"Sarah Michelle Gellar", "CA", "CA", "Canada", 1L},
            new Object[]{"DirecTV", "US", "US", "United States", 13L},
            new Object[]{"Carlo Curti", "US", "US", "United States", 13L},
            new Object[]{"Giusy Ferreri discography", "IT", "IT", "Italy", 7L},
            new Object[]{"Roma-Bangkok", "IT", "IT", "Italy", 7L},
            new Object[]{"Wendigo", "SV", "SV", "El Salvador", 12L},
            new Object[]{"Алиса в Зазеркалье", "NO", "NO", "Norway", 11L},
            new Object[]{"Gabinete Ministerial de Rafael Correa", "EC", "EC", "Ecuador", 4L},
            new Object[]{"Old Anatolian Turkish", "US", "US", "United States", 13L}
        )
    );
  }

  @Test
  public void test_makeCursors_factToCountryInnerUsingLookup()
  {
    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(factToCountryNameUsingIsoCodeLookup(JoinType.INNER))
        ).makeCursors(
            null,
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "k",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "v"
        ),
        ImmutableList.of(
            new Object[]{"Peremptory norm", "AU", "AU", "Australia"},
            new Object[]{"Mathis Bolly", "MX", "MX", "Mexico"},
            new Object[]{"유희왕 GX", "KR", "KR", "Republic of Korea"},
            new Object[]{"青野武", "JP", "JP", "Japan"},
            new Object[]{"Golpe de Estado en Chile de 1973", "CL", "CL", "Chile"},
            new Object[]{"President of India", "US", "US", "United States"},
            new Object[]{"Diskussion:Sebastian Schulz", "DE", "DE", "Germany"},
            new Object[]{"Saison 9 de Secret Story", "FR", "FR", "France"},
            new Object[]{"Glasgow", "GB", "GB", "United Kingdom"},
            new Object[]{"Didier Leclair", "CA", "CA", "Canada"},
            new Object[]{"Les Argonautes", "CA", "CA", "Canada"},
            new Object[]{"Otjiwarongo Airport", "US", "US", "United States"},
            new Object[]{"Sarah Michelle Gellar", "CA", "CA", "Canada"},
            new Object[]{"DirecTV", "US", "US", "United States"},
            new Object[]{"Carlo Curti", "US", "US", "United States"},
            new Object[]{"Giusy Ferreri discography", "IT", "IT", "Italy"},
            new Object[]{"Roma-Bangkok", "IT", "IT", "Italy"},
            new Object[]{"Wendigo", "SV", "SV", "El Salvador"},
            new Object[]{"Алиса в Зазеркалье", "NO", "NO", "Norway"},
            new Object[]{"Gabinete Ministerial de Rafael Correa", "EC", "EC", "Ecuador"},
            new Object[]{"Old Anatolian Turkish", "US", "US", "United States"}
        )
    );
  }

  @Test
  public void test_makeCursors_factToCountryInnerUsingCountryNumber()
  {
    // In non-SQL-compatible mode, we get an extra row, since the 'null' countryNumber for "Talk:Oswald Tilghman"
    // is interpreted as 0 (a.k.a. Australia).

    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(factToCountryOnNumber(JoinType.INNER))
        ).makeCursors(
            new SelectorDimFilter("channel", "#en.wikipedia", null).toFilter(),
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            "countryIsoCode",
            FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "countryIsoCode",
            FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "countryName",
            FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "countryNumber"
        ),
        NullHandling.sqlCompatible() ?
        ImmutableList.of(
            new Object[]{"Peremptory norm", "AU", "AU", "Australia", 0L},
            new Object[]{"President of India", "US", "US", "United States", 13L},
            new Object[]{"Glasgow", "GB", "GB", "United Kingdom", 6L},
            new Object[]{"Otjiwarongo Airport", "US", "US", "United States", 13L},
            new Object[]{"Sarah Michelle Gellar", "CA", "CA", "Canada", 1L},
            new Object[]{"DirecTV", "US", "US", "United States", 13L},
            new Object[]{"Carlo Curti", "US", "US", "United States", 13L},
            new Object[]{"Giusy Ferreri discography", "IT", "IT", "Italy", 7L},
            new Object[]{"Roma-Bangkok", "IT", "IT", "Italy", 7L},
            new Object[]{"Old Anatolian Turkish", "US", "US", "United States", 13L}
        ) :
        ImmutableList.of(
            new Object[]{"Talk:Oswald Tilghman", null, "AU", "Australia", 0L},
            new Object[]{"Peremptory norm", "AU", "AU", "Australia", 0L},
            new Object[]{"President of India", "US", "US", "United States", 13L},
            new Object[]{"Glasgow", "GB", "GB", "United Kingdom", 6L},
            new Object[]{"Otjiwarongo Airport", "US", "US", "United States", 13L},
            new Object[]{"Sarah Michelle Gellar", "CA", "CA", "Canada", 1L},
            new Object[]{"DirecTV", "US", "US", "United States", 13L},
            new Object[]{"Carlo Curti", "US", "US", "United States", 13L},
            new Object[]{"Giusy Ferreri discography", "IT", "IT", "Italy", 7L},
            new Object[]{"Roma-Bangkok", "IT", "IT", "Italy", 7L},
            new Object[]{"Old Anatolian Turkish", "US", "US", "United States", 13L}
        )
    );
  }

  @Test
  public void test_makeCursors_factToCountryInnerUsingCountryNumberUsingLookup()
  {
    // In non-SQL-compatible mode, we get an extra row, since the 'null' countryNumber for "Talk:Oswald Tilghman"
    // is interpreted as 0 (a.k.a. Australia).

    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(factToCountryNameUsingNumberLookup(JoinType.INNER))
        ).makeCursors(
            new SelectorDimFilter("channel", "#en.wikipedia", null).toFilter(),
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            "countryIsoCode",
            FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "v"
        ),
        NullHandling.sqlCompatible() ?
        ImmutableList.of(
            new Object[]{"Peremptory norm", "AU", "Australia"},
            new Object[]{"President of India", "US", "United States"},
            new Object[]{"Glasgow", "GB", "United Kingdom"},
            new Object[]{"Otjiwarongo Airport", "US", "United States"},
            new Object[]{"Sarah Michelle Gellar", "CA", "Canada"},
            new Object[]{"DirecTV", "US", "United States"},
            new Object[]{"Carlo Curti", "US", "United States"},
            new Object[]{"Giusy Ferreri discography", "IT", "Italy"},
            new Object[]{"Roma-Bangkok", "IT", "Italy"},
            new Object[]{"Old Anatolian Turkish", "US", "United States"}
        ) :
        ImmutableList.of(
            new Object[]{"Talk:Oswald Tilghman", null, "Australia"},
            new Object[]{"Peremptory norm", "AU", "Australia"},
            new Object[]{"President of India", "US", "United States"},
            new Object[]{"Glasgow", "GB", "United Kingdom"},
            new Object[]{"Otjiwarongo Airport", "US", "United States"},
            new Object[]{"Sarah Michelle Gellar", "CA", "Canada"},
            new Object[]{"DirecTV", "US", "United States"},
            new Object[]{"Carlo Curti", "US", "United States"},
            new Object[]{"Giusy Ferreri discography", "IT", "Italy"},
            new Object[]{"Roma-Bangkok", "IT", "Italy"},
            new Object[]{"Old Anatolian Turkish", "US", "United States"}
        )
    );
  }

  @Test
  public void test_makeCursors_factToCountryLeftWithFilterOnFacts()
  {
    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(factToCountryOnIsoCode(JoinType.LEFT))
        ).makeCursors(
            new SelectorDimFilter("channel", "#de.wikipedia", null).toFilter(),
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryNumber"
        ),
        ImmutableList.of(
            new Object[]{"Diskussion:Sebastian Schulz", "DE", "DE", "Germany", 3L}
        )
    );
  }

  @Test
  public void test_makeCursors_factToCountryRightWithFilterOnLeftIsNull()
  {
    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(factToCountryOnIsoCode(JoinType.RIGHT))
        ).makeCursors(
            new SelectorDimFilter("channel", null, null).toFilter(),
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            "countryIsoCode",
            "countryNumber",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryNumber"
        ),
        ImmutableList.of(
            new Object[]{null, null, NullHandling.sqlCompatible() ? null : 0L, "AX", "Atlantis", 14L}
        )
    );
  }

  @Test
  public void test_makeCursors_factToCountryRightWithFilterOnJoinable()
  {
    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(factToCountryOnIsoCode(JoinType.RIGHT))
        ).makeCursors(
            new SelectorDimFilter(
                FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName",
                "Germany",
                null
            ).toFilter(),
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            "countryIsoCode",
            "countryNumber",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryNumber"
        ),
        ImmutableList.of(
            new Object[]{"Diskussion:Sebastian Schulz", "DE", 3L, "DE", "Germany", 3L}
        )
    );
  }

  @Test
  public void test_makeCursors_factToCountryLeftWithFilterOnJoinable()
  {
    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(factToCountryOnIsoCode(JoinType.LEFT))
        ).makeCursors(
            new OrDimFilter(
                new SelectorDimFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryIsoCode", "DE", null),
                new SelectorDimFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName", "Norway", null),
                new SelectorDimFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryNumber", "10", null)
            ).toFilter(),
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryNumber"
        ),
        ImmutableList.of(
            new Object[]{"Mathis Bolly", "MX", "MX", "Mexico", 10L},
            new Object[]{"Diskussion:Sebastian Schulz", "DE", "DE", "Germany", 3L},
            new Object[]{"Алиса в Зазеркалье", "NO", "NO", "Norway", 11L}
        )
    );
  }

  @Test
  public void test_makeCursors_factToCountryLeftWithFilterOnJoinableUsingLookup()
  {
    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(factToCountryNameUsingIsoCodeLookup(JoinType.LEFT))
        ).makeCursors(
            new OrDimFilter(
                new SelectorDimFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "k", "DE", null),
                new SelectorDimFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "v", "Norway", null)
            ).toFilter(),
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "k",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "v"
        ),
        ImmutableList.of(
            new Object[]{"Diskussion:Sebastian Schulz", "DE", "DE", "Germany"},
            new Object[]{"Алиса в Зазеркалье", "NO", "NO", "Norway"}
        )
    );
  }

  @Test
  public void test_makeCursors_factToCountryInnerWithFilterInsteadOfRealJoinCondition()
  {
    // Join condition => always true.
    // Filter => Fact to countries on countryIsoCode.

    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(
                new JoinableClause(
                    FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
                    new IndexedTableJoinable(countriesTable),
                    JoinType.INNER,
                    JoinConditionAnalysis.forExpression(
                        "1",
                        FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
                        ExprMacroTable.nil()
                    )
                )
            )
        ).makeCursors(
            new ExpressionDimFilter(
                StringUtils.format("\"%scountryIsoCode\" == countryIsoCode", FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX),
                ExprMacroTable.nil()
            ).toFilter(),
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryNumber"
        ),
        ImmutableList.of(
            new Object[]{"Peremptory norm", "AU", "AU", "Australia", 0L},
            new Object[]{"Mathis Bolly", "MX", "MX", "Mexico", 10L},
            new Object[]{"유희왕 GX", "KR", "KR", "Republic of Korea", 9L},
            new Object[]{"青野武", "JP", "JP", "Japan", 8L},
            new Object[]{"Golpe de Estado en Chile de 1973", "CL", "CL", "Chile", 2L},
            new Object[]{"President of India", "US", "US", "United States", 13L},
            new Object[]{"Diskussion:Sebastian Schulz", "DE", "DE", "Germany", 3L},
            new Object[]{"Saison 9 de Secret Story", "FR", "FR", "France", 5L},
            new Object[]{"Glasgow", "GB", "GB", "United Kingdom", 6L},
            new Object[]{"Didier Leclair", "CA", "CA", "Canada", 1L},
            new Object[]{"Les Argonautes", "CA", "CA", "Canada", 1L},
            new Object[]{"Otjiwarongo Airport", "US", "US", "United States", 13L},
            new Object[]{"Sarah Michelle Gellar", "CA", "CA", "Canada", 1L},
            new Object[]{"DirecTV", "US", "US", "United States", 13L},
            new Object[]{"Carlo Curti", "US", "US", "United States", 13L},
            new Object[]{"Giusy Ferreri discography", "IT", "IT", "Italy", 7L},
            new Object[]{"Roma-Bangkok", "IT", "IT", "Italy", 7L},
            new Object[]{"Wendigo", "SV", "SV", "El Salvador", 12L},
            new Object[]{"Алиса в Зазеркалье", "NO", "NO", "Norway", 11L},
            new Object[]{"Gabinete Ministerial de Rafael Correa", "EC", "EC", "Ecuador", 4L},
            new Object[]{"Old Anatolian Turkish", "US", "US", "United States", 13L}
        )
    );
  }

  @Test
  public void test_makeCursors_factToRegionToCountryLeft()
  {
    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(
                factToRegion(JoinType.LEFT),
                regionToCountry(JoinType.LEFT)
            )
        ).makeCursors(
            null,
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            FACT_TO_REGION_PREFIX + "regionName",
            REGION_TO_COUNTRY_PREFIX + "countryName"
        ),
        ImmutableList.of(
            new Object[]{"Talk:Oswald Tilghman", null, null},
            new Object[]{"Rallicula", null, null},
            new Object[]{"Peremptory norm", "New South Wales", "Australia"},
            new Object[]{"Apamea abruzzorum", null, null},
            new Object[]{"Atractus flammigerus", null, null},
            new Object[]{"Agama mossambica", null, null},
            new Object[]{"Mathis Bolly", "Mexico City", "Mexico"},
            new Object[]{"유희왕 GX", "Seoul", "Republic of Korea"},
            new Object[]{"青野武", "Tōkyō", "Japan"},
            new Object[]{"Golpe de Estado en Chile de 1973", "Santiago Metropolitan", "Chile"},
            new Object[]{"President of India", "California", "United States"},
            new Object[]{"Diskussion:Sebastian Schulz", "Hesse", "Germany"},
            new Object[]{"Saison 9 de Secret Story", "Val d'Oise", "France"},
            new Object[]{"Glasgow", "Kingston upon Hull", "United Kingdom"},
            new Object[]{"Didier Leclair", "Ontario", "Canada"},
            new Object[]{"Les Argonautes", "Quebec", "Canada"},
            new Object[]{"Otjiwarongo Airport", "California", "United States"},
            new Object[]{"Sarah Michelle Gellar", "Ontario", "Canada"},
            new Object[]{"DirecTV", "North Carolina", "United States"},
            new Object[]{"Carlo Curti", "California", "United States"},
            new Object[]{"Giusy Ferreri discography", "Provincia di Varese", "Italy"},
            new Object[]{"Roma-Bangkok", "Provincia di Varese", "Italy"},
            new Object[]{"Wendigo", "Departamento de San Salvador", "El Salvador"},
            new Object[]{"Алиса в Зазеркалье", "Finnmark Fylke", "Norway"},
            new Object[]{"Gabinete Ministerial de Rafael Correa", "Provincia del Guayas", "Ecuador"},
            new Object[]{"Old Anatolian Turkish", "Virginia", "United States"}
        )
    );
  }

  @Test
  public void test_makeCursors_factToCountryAlwaysTrue()
  {
    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(
                new JoinableClause(
                    FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
                    new IndexedTableJoinable(countriesTable),
                    JoinType.LEFT,
                    JoinConditionAnalysis.forExpression(
                        "1",
                        FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
                        ExprMacroTable.nil()
                    )
                )
            )
        ).makeCursors(
            new SelectorDimFilter("channel", "#de.wikipedia", null).toFilter(),
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName"
        ),
        ImmutableList.of(
            new Object[]{"Diskussion:Sebastian Schulz", "Australia"},
            new Object[]{"Diskussion:Sebastian Schulz", "Canada"},
            new Object[]{"Diskussion:Sebastian Schulz", "Chile"},
            new Object[]{"Diskussion:Sebastian Schulz", "Germany"},
            new Object[]{"Diskussion:Sebastian Schulz", "Ecuador"},
            new Object[]{"Diskussion:Sebastian Schulz", "France"},
            new Object[]{"Diskussion:Sebastian Schulz", "United Kingdom"},
            new Object[]{"Diskussion:Sebastian Schulz", "Italy"},
            new Object[]{"Diskussion:Sebastian Schulz", "Japan"},
            new Object[]{"Diskussion:Sebastian Schulz", "Republic of Korea"},
            new Object[]{"Diskussion:Sebastian Schulz", "Mexico"},
            new Object[]{"Diskussion:Sebastian Schulz", "Norway"},
            new Object[]{"Diskussion:Sebastian Schulz", "El Salvador"},
            new Object[]{"Diskussion:Sebastian Schulz", "United States"},
            new Object[]{"Diskussion:Sebastian Schulz", "Atlantis"}
        )
    );
  }

  @Test
  public void test_makeCursors_factToCountryAlwaysFalse()
  {
    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(
                new JoinableClause(
                    FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
                    new IndexedTableJoinable(countriesTable),
                    JoinType.LEFT,
                    JoinConditionAnalysis.forExpression(
                        "0",
                        FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
                        ExprMacroTable.nil()
                    )
                )
            )
        ).makeCursors(
            new SelectorDimFilter("channel", "#de.wikipedia", null).toFilter(),
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName"
        ),
        ImmutableList.of(
            new Object[]{"Diskussion:Sebastian Schulz", null}
        )
    );
  }

  @Test
  public void test_makeCursors_factToCountryAlwaysTrueUsingLookup()
  {
    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(
                new JoinableClause(
                    FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
                    LookupJoinable.wrap(countryIsoCodeToNameLookup),
                    JoinType.LEFT,
                    JoinConditionAnalysis.forExpression(
                        "1",
                        FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
                        ExprMacroTable.nil()
                    )
                )
            )
        ).makeCursors(
            new SelectorDimFilter("channel", "#de.wikipedia", null).toFilter(),
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "v"
        ),
        ImmutableList.of(
            new Object[]{"Diskussion:Sebastian Schulz", "Australia"},
            new Object[]{"Diskussion:Sebastian Schulz", "Canada"},
            new Object[]{"Diskussion:Sebastian Schulz", "Chile"},
            new Object[]{"Diskussion:Sebastian Schulz", "Germany"},
            new Object[]{"Diskussion:Sebastian Schulz", "Ecuador"},
            new Object[]{"Diskussion:Sebastian Schulz", "France"},
            new Object[]{"Diskussion:Sebastian Schulz", "United Kingdom"},
            new Object[]{"Diskussion:Sebastian Schulz", "Italy"},
            new Object[]{"Diskussion:Sebastian Schulz", "Japan"},
            new Object[]{"Diskussion:Sebastian Schulz", "Republic of Korea"},
            new Object[]{"Diskussion:Sebastian Schulz", "Mexico"},
            new Object[]{"Diskussion:Sebastian Schulz", "Norway"},
            new Object[]{"Diskussion:Sebastian Schulz", "El Salvador"},
            new Object[]{"Diskussion:Sebastian Schulz", "United States"},
            new Object[]{"Diskussion:Sebastian Schulz", "Atlantis"}
        )
    );
  }

  @Test
  public void test_makeCursors_factToCountryAlwaysFalseUsingLookup()
  {
    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(
                new JoinableClause(
                    FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
                    LookupJoinable.wrap(countryIsoCodeToNameLookup),
                    JoinType.LEFT,
                    JoinConditionAnalysis.forExpression(
                        "0",
                        FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
                        ExprMacroTable.nil()
                    )
                )
            )
        ).makeCursors(
            new SelectorDimFilter("channel", "#de.wikipedia", null).toFilter(),
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "v"
        ),
        ImmutableList.of(
            new Object[]{"Diskussion:Sebastian Schulz", null}
        )
    );
  }

  @Test
  public void test_makeCursors_factToCountryUsingVirtualColumn()
  {
    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(
                new JoinableClause(
                    FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
                    new IndexedTableJoinable(countriesTable),
                    JoinType.INNER,
                    JoinConditionAnalysis.forExpression(
                        StringUtils.format("\"%scountryIsoCode\" == virtual", FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX),
                        FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
                        ExprMacroTable.nil()
                    )
                )
            )
        ).makeCursors(
            null,
            Intervals.ETERNITY,
            VirtualColumns.create(
                Collections.singletonList(
                    new ExpressionVirtualColumn(
                        "virtual",
                        "concat(substring(countryIsoCode, 0, 1),'L')",
                        ValueType.STRING,
                        ExprMacroTable.nil()
                    )
                )
            ),
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            "countryIsoCode",
            "virtual",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName"
        ),
        ImmutableList.of(
            new Object[]{"Golpe de Estado en Chile de 1973", "CL", "CL", "CL", "Chile"},
            new Object[]{"Didier Leclair", "CA", "CL", "CL", "Chile"},
            new Object[]{"Les Argonautes", "CA", "CL", "CL", "Chile"},
            new Object[]{"Sarah Michelle Gellar", "CA", "CL", "CL", "Chile"}
        )
    );
  }

  @Test
  public void test_makeCursors_factToCountryUsingExpression()
  {
    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(
                new JoinableClause(
                    FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
                    new IndexedTableJoinable(countriesTable),
                    JoinType.INNER,
                    JoinConditionAnalysis.forExpression(
                        StringUtils.format(
                            "\"%scountryIsoCode\" == concat(substring(countryIsoCode, 0, 1),'L')",
                            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX
                        ),
                        FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
                        ExprMacroTable.nil()
                    )
                )
            )
        ).makeCursors(
            null,
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName"
        ),
        ImmutableList.of(
            new Object[]{"Golpe de Estado en Chile de 1973", "CL", "CL", "Chile"},
            new Object[]{"Didier Leclair", "CA", "CL", "Chile"},
            new Object[]{"Les Argonautes", "CA", "CL", "Chile"},
            new Object[]{"Sarah Michelle Gellar", "CA", "CL", "Chile"}
        )
    );
  }

  @Test
  public void test_makeCursors_factToRegionTheWrongWay()
  {
    // Joins using only regionIsoCode, which is wrong since they are not unique internationally.

    JoinTestHelper.verifyCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(
                new JoinableClause(
                    FACT_TO_REGION_PREFIX,
                    new IndexedTableJoinable(regionsTable),
                    JoinType.LEFT,
                    JoinConditionAnalysis.forExpression(
                        StringUtils.format(
                            "\"%sregionIsoCode\" == regionIsoCode",
                            FACT_TO_REGION_PREFIX
                        ),
                        FACT_TO_REGION_PREFIX,
                        ExprMacroTable.nil()
                    )
                )
            )
        ).makeCursors(
            new SelectorDimFilter("regionIsoCode", "VA", null).toFilter(),
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            "regionIsoCode",
            "countryIsoCode",
            FACT_TO_REGION_PREFIX + "regionName",
            FACT_TO_REGION_PREFIX + "countryIsoCode"
        ),
        ImmutableList.of(
            new Object[]{"Giusy Ferreri discography", "VA", "IT", "Provincia di Varese", "IT"},
            new Object[]{"Giusy Ferreri discography", "VA", "IT", "Virginia", "US"},
            new Object[]{"Roma-Bangkok", "VA", "IT", "Provincia di Varese", "IT"},
            new Object[]{"Roma-Bangkok", "VA", "IT", "Virginia", "US"},
            new Object[]{"Old Anatolian Turkish", "VA", "US", "Provincia di Varese", "IT"},
            new Object[]{"Old Anatolian Turkish", "VA", "US", "Virginia", "US"}
        )
    );
  }

  @Test
  public void test_makeCursors_errorOnNonEquiJoin()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Cannot build hash-join matcher on non-equi-join condition: x == y");

    JoinTestHelper.readCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(
                new JoinableClause(
                    FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
                    new IndexedTableJoinable(countriesTable),
                    JoinType.LEFT,
                    JoinConditionAnalysis.forExpression(
                        "x == y",
                        FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
                        ExprMacroTable.nil()
                    )
                )
            )
        ).makeCursors(
            null,
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void test_makeCursors_errorOnNonKeyBasedJoin()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Cannot build hash-join matcher on non-key-based condition: "
                                    + "Equality{leftExpr=x, rightColumn='countryName'}");

    JoinTestHelper.readCursors(
        new HashJoinSegmentStorageAdapter(
            factSegment.asStorageAdapter(),
            ImmutableList.of(
                new JoinableClause(
                    FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
                    new IndexedTableJoinable(countriesTable),
                    JoinType.LEFT,
                    JoinConditionAnalysis.forExpression(
                        StringUtils.format("x == \"%scountryName\"", FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX),
                        FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
                        ExprMacroTable.nil()
                    )
                )
            )
        ).makeCursors(
            null,
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of()
    );
  }

  private JoinableClause factToCountryNameUsingIsoCodeLookup(final JoinType joinType)
  {
    return new JoinableClause(
        FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
        LookupJoinable.wrap(countryIsoCodeToNameLookup),
        joinType,
        JoinConditionAnalysis.forExpression(
            StringUtils.format("\"%sk\" == countryIsoCode", FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX),
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
            ExprMacroTable.nil()
        )
    );
  }

  private JoinableClause factToCountryNameUsingNumberLookup(final JoinType joinType)
  {
    return new JoinableClause(
        FACT_TO_COUNTRY_ON_NUMBER_PREFIX,
        LookupJoinable.wrap(countryNumberToNameLookup),
        joinType,
        JoinConditionAnalysis.forExpression(
            StringUtils.format("\"%sk\" == countryNumber", FACT_TO_COUNTRY_ON_NUMBER_PREFIX),
            FACT_TO_COUNTRY_ON_NUMBER_PREFIX,
            ExprMacroTable.nil()
        )
    );
  }

  private JoinableClause factToCountryOnIsoCode(final JoinType joinType)
  {
    return new JoinableClause(
        FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
        new IndexedTableJoinable(countriesTable),
        joinType,
        JoinConditionAnalysis.forExpression(
            StringUtils.format("\"%scountryIsoCode\" == countryIsoCode", FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX),
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
            ExprMacroTable.nil()
        )
    );
  }

  private JoinableClause factToCountryOnNumber(final JoinType joinType)
  {
    return new JoinableClause(
        FACT_TO_COUNTRY_ON_NUMBER_PREFIX,
        new IndexedTableJoinable(countriesTable),
        joinType,
        JoinConditionAnalysis.forExpression(
            StringUtils.format("\"%scountryNumber\" == countryNumber", FACT_TO_COUNTRY_ON_NUMBER_PREFIX),
            FACT_TO_COUNTRY_ON_NUMBER_PREFIX,
            ExprMacroTable.nil()
        )
    );
  }

  private JoinableClause factToRegion(final JoinType joinType)
  {
    return new JoinableClause(
        FACT_TO_REGION_PREFIX,
        new IndexedTableJoinable(regionsTable),
        joinType,
        JoinConditionAnalysis.forExpression(
            StringUtils.format(
                "\"%sregionIsoCode\" == regionIsoCode && \"%scountryIsoCode\" == countryIsoCode",
                FACT_TO_REGION_PREFIX,
                FACT_TO_REGION_PREFIX
            ),
            FACT_TO_REGION_PREFIX,
            ExprMacroTable.nil()
        )
    );
  }

  private JoinableClause regionToCountry(final JoinType joinType)
  {
    return new JoinableClause(
        REGION_TO_COUNTRY_PREFIX,
        new IndexedTableJoinable(countriesTable),
        joinType,
        JoinConditionAnalysis.forExpression(
            StringUtils.format(
                "\"%scountryIsoCode\" == \"%scountryIsoCode\"",
                FACT_TO_REGION_PREFIX,
                REGION_TO_COUNTRY_PREFIX
            ),
            REGION_TO_COUNTRY_PREFIX,
            ExprMacroTable.nil()
        )
    );
  }

  private HashJoinSegmentStorageAdapter makeFactToCountrySegment()
  {
    return new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        ImmutableList.of(factToCountryOnIsoCode(JoinType.LEFT))
    );
  }
}
