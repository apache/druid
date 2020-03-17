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
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.BoundFilter;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.segment.join.filter.JoinFilterAnalyzer;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.segment.join.filter.JoinFilterSplit;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class JoinFilterAnalyzerTest extends BaseHashJoinSegmentStorageAdapterTest
{
  @Test
  public void test_filterPushDown_factToRegionToCountryLeftFilterOnChannel()
  {
    Filter originalFilter = new SelectorFilter("channel", "#en.wikipedia");
    List<JoinableClause> joinableClauses = ImmutableList.of(
        factToRegion(JoinType.LEFT),
        regionToCountry(JoinType.LEFT)
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );

    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new SelectorFilter("channel", "#en.wikipedia"),
        null,
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
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
            new Object[]{"Peremptory norm", "New South Wales", "Australia"},
            new Object[]{"President of India", "California", "United States"},
            new Object[]{"Glasgow", "Kingston upon Hull", "United Kingdom"},
            new Object[]{"Otjiwarongo Airport", "California", "United States"},
            new Object[]{"Sarah Michelle Gellar", "Ontario", "Canada"},
            new Object[]{"DirecTV", "North Carolina", "United States"},
            new Object[]{"Carlo Curti", "California", "United States"},
            new Object[]{"Giusy Ferreri discography", "Provincia di Varese", "Italy"},
            new Object[]{"Roma-Bangkok", "Provincia di Varese", "Italy"},
            new Object[]{"Old Anatolian Turkish", "Virginia", "United States"},
            new Object[]{"Cream Soda", "Ainigriv", "States United"},
            new Object[]{"Orange Soda", null, null},
            new Object[]{"History of Fourems", "Fourems Province", "Fourems"}
        )
    );
  }


  @Test
  public void test_filterPushDown_factToRegionExprToCountryLeftFilterOnCountryName()
  {

    Filter originalFilter = new SelectorFilter("rtc.countryName", "United States");
    JoinableClause regionExprToCountry = new JoinableClause(
        REGION_TO_COUNTRY_PREFIX,
        new IndexedTableJoinable(countriesTable),
        JoinType.LEFT,
        JoinConditionAnalysis.forExpression(
            StringUtils.format(
                "reverse(\"%scountryIsoCode\") == \"%scountryIsoCode\"",
                FACT_TO_REGION_PREFIX,
                REGION_TO_COUNTRY_PREFIX
            ),
            REGION_TO_COUNTRY_PREFIX,
            ExprMacroTable.nil()
        )
    );
    List<JoinableClause> joinableClauses = ImmutableList.of(
        factToRegion(JoinType.LEFT),
        regionExprToCountry
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );

    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        null,
        new SelectorFilter("rtc.countryName", "United States"),
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
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
            new Object[]{"Cream Soda", "Ainigriv", "United States"}
        )
    );
  }

  @Test
  public void test_filterPushDown_factToRegionToCountryLeftFilterOnChannelAndCountryName()
  {
    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", "#en.wikipedia"),
            new SelectorFilter("rtc.countryName", "United States")
        )
    );

    List<JoinableClause> joinableClauses = ImmutableList.of(
        factToRegion(JoinType.LEFT),
        regionToCountry(JoinType.LEFT)
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );

    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", "#en.wikipedia"),
                new InDimFilter("countryIsoCode", ImmutableSet.of("US"), null, null).toFilter()
            )
        ),
        new SelectorFilter("rtc.countryName", "United States"),
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
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
            new Object[]{"President of India", "California", "United States"},
            new Object[]{"Otjiwarongo Airport", "California", "United States"},
            new Object[]{"DirecTV", "North Carolina", "United States"},
            new Object[]{"Carlo Curti", "California", "United States"},
            new Object[]{"Old Anatolian Turkish", "Virginia", "United States"}
        )
    );
  }

  @Test
  public void test_filterPushDown_factToRegionToCountryLeftFilterOnNullColumns()
  {
    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("countryIsoCode", null),
            new SelectorFilter("countryNumber", null),
            new SelectorFilter("rtc.countryName", null),
            new SelectorFilter("r1.regionName", null)
        )
    );

    List<JoinableClause> joinableClauses = ImmutableList.of(
        factToRegion(JoinType.LEFT),
        regionToCountry(JoinType.LEFT)
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );

    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        null,
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("countryIsoCode", null),
                new SelectorFilter("countryNumber", null),
                new SelectorFilter("rtc.countryName", null),
                new SelectorFilter("r1.regionName", null)
            )
        ),
        ImmutableList.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
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
        NullHandling.sqlCompatible() ?
        ImmutableList.of(
            new Object[]{"Talk:Oswald Tilghman", null, null},
            new Object[]{"Rallicula", null, null},
            new Object[]{"Apamea abruzzorum", null, null},
            new Object[]{"Atractus flammigerus", null, null},
            new Object[]{"Agama mossambica", null, null}
        ) :
        ImmutableList.of() // when not running in SQL compatible mode, countryNumber does not have nulls
    );
  }

  @Test
  public void test_filterPushDown_factToRegionToCountryLeftFilterOnInvalidColumns()
  {
    List<JoinableClause> joinableClauses = ImmutableList.of(
        factToRegion(JoinType.LEFT),
        regionToCountry(JoinType.LEFT)
    );

    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("baseTableInvalidColumn", "abcd"),
            new SelectorFilter("rtc.invalidColumn", "abcd"),
            new SelectorFilter("r1.invalidColumn", "abcd")
        )
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );

    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new SelectorFilter("baseTableInvalidColumn", "abcd"),
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("rtc.invalidColumn", "abcd"),
                new SelectorFilter("r1.invalidColumn", "abcd")
            )
        ),
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
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
        ImmutableList.of()
    );
  }

  @Test
  public void test_filterPushDown_factToRegionToCountryLeftFilterOnChannelVirtualColumn()
  {
    List<JoinableClause> joinableClauses = ImmutableList.of(
        factToRegion(JoinType.LEFT),
        regionToCountry(JoinType.LEFT)
    );

    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("v1", "virtual-column-#en.wikipedia")
        )
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );

    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    VirtualColumns virtualColumns = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn(
                "v1",
                "concat('virtual-column-', \"channel\")",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new SelectorFilter("v1", "virtual-column-#en.wikipedia"),
        null,
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
            Intervals.ETERNITY,
            virtualColumns,
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
            new Object[]{"Peremptory norm", "New South Wales", "Australia"},
            new Object[]{"President of India", "California", "United States"},
            new Object[]{"Glasgow", "Kingston upon Hull", "United Kingdom"},
            new Object[]{"Otjiwarongo Airport", "California", "United States"},
            new Object[]{"Sarah Michelle Gellar", "Ontario", "Canada"},
            new Object[]{"DirecTV", "North Carolina", "United States"},
            new Object[]{"Carlo Curti", "California", "United States"},
            new Object[]{"Giusy Ferreri discography", "Provincia di Varese", "Italy"},
            new Object[]{"Roma-Bangkok", "Provincia di Varese", "Italy"},
            new Object[]{"Old Anatolian Turkish", "Virginia", "United States"},
            new Object[]{"Cream Soda", "Ainigriv", "States United"},
            new Object[]{"Orange Soda", null, null},
            new Object[]{"History of Fourems", "Fourems Province", "Fourems"}
        )
    );
  }

  @Test
  public void test_filterPushDown_factToRegionFilterOnRHSRegionNameExprVirtualColumn()
  {
    // If our query had a filter that uses expressions, such as upper(r1.regionName) == 'VIRGINIA', this plans into
    // a selector filter on an ExpressionVirtualColumn
    Filter originalFilter = new SelectorFilter("v0", "VIRGINIA");
    VirtualColumns virtualColumns = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn(
                "v0",
                "upper(\"r1.regionName\")",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
    );

    List<JoinableClause> joinableClauses = ImmutableList.of(
        factToRegion(JoinType.LEFT)
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
        joinableClauses,
        virtualColumns,
        originalFilter,
        true,
        true,
        true,
        QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE_KEY
    );

    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        null,
        new SelectorFilter("v0", "VIRGINIA"),
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
            Intervals.ETERNITY,
            virtualColumns,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            "v0"
        ),
        ImmutableList.of(
            new Object[]{"Old Anatolian Turkish", "VIRGINIA"}
        )
    );
  }


  @Test
  public void test_filterPushDown_factToRegionToCountryLeftFilterNormalizedAlreadyPushDownVariety()
  {
    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", "#fr.wikipedia"),
            new BoundFilter(new BoundDimFilter(
                "page",
                "Les Argonautes",
                "Les Argonautes",
                false,
                false,
                null,
                null,
                null
            )),
            new SelectorFilter("rtc.countryName", "Canada"),
            new BoundFilter(new BoundDimFilter(
                "rtc.countryName",
                "Canada",
                "Canada",
                false,
                false,
                null,
                null,
                null
            )),
            new OrFilter(
                ImmutableList.of(
                    new SelectorFilter("namespace", "main"),
                    new BoundFilter(new BoundDimFilter(
                        "user",
                        "24.122.168.111",
                        "24.122.168.111",
                        false,
                        false,
                        null,
                        null,
                        null
                    ))
                )
            ),
            new OrFilter(
                ImmutableList.of(
                    new SelectorFilter("namespace", "main"),
                    new BoundFilter(new BoundDimFilter(
                        "r1.regionName",
                        "Quebec",
                        "Quebec",
                        false,
                        false,
                        null,
                        null,
                        null
                    ))
                )
            )
        )
    );

    List<JoinableClause> joinableClauses = ImmutableList.of(
        factToRegion(JoinType.LEFT),
        regionToCountry(JoinType.LEFT)
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", "#fr.wikipedia"),
                new BoundFilter(new BoundDimFilter(
                    "page",
                    "Les Argonautes",
                    "Les Argonautes",
                    false,
                    false,
                    null,
                    null,
                    null
                )),
                new OrFilter(
                    ImmutableList.of(
                        new SelectorFilter("namespace", "main"),
                        new BoundFilter(new BoundDimFilter(
                            "user",
                            "24.122.168.111",
                            "24.122.168.111",
                            false,
                            false,
                            null,
                            null,
                            null
                        ))
                    )
                ),
                new InDimFilter("countryIsoCode", ImmutableSet.of("CA"), null, null).toFilter()
            )
        ),
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("rtc.countryName", "Canada"),
                new BoundFilter(new BoundDimFilter(
                    "rtc.countryName",
                    "Canada",
                    "Canada",
                    false,
                    false,
                    null,
                    null,
                    null
                )),
                new OrFilter(
                    ImmutableList.of(
                        new SelectorFilter("namespace", "main"),
                        new BoundFilter(new BoundDimFilter(
                            "r1.regionName",
                            "Quebec",
                            "Quebec",
                            false,
                            false,
                            null,
                            null,
                            null
                        ))
                    )
                )
            )
        ),
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
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
            new Object[]{"Les Argonautes", "Quebec", "Canada"}
        )
    );
  }

  @Test
  public void test_filterPushDown_factExpressionsToRegionToCountryLeftFilterOnChannelAndCountryName()
  {
    JoinableClause factExprToRegon = new JoinableClause(
        FACT_TO_REGION_PREFIX,
        new IndexedTableJoinable(regionsTable),
        JoinType.LEFT,
        JoinConditionAnalysis.forExpression(
            StringUtils.format(
                "\"%sregionIsoCode\" == reverse(regionIsoCode) && \"%scountryIsoCode\" == reverse(countryIsoCode)",
                FACT_TO_REGION_PREFIX,
                FACT_TO_REGION_PREFIX
            ),
            FACT_TO_REGION_PREFIX,
            ExprMacroTable.nil()
        )
    );
    List<JoinableClause> joinableClauses = ImmutableList.of(
        factExprToRegon,
        regionToCountry(JoinType.LEFT)
    );
    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", "#en.wikipedia"),
            new SelectorFilter("rtc.countryName", "States United")
        )
    );
    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );

    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", "#en.wikipedia"),
                new InDimFilter("JOIN-FILTER-PUSHDOWN-VIRTUAL-COLUMN-0", ImmutableSet.of("SU"), null, null).toFilter()
            )
        ),
        new SelectorFilter("rtc.countryName", "States United"),
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);

    ExpressionVirtualColumn expectedVirtualColumn = new ExpressionVirtualColumn(
        "JOIN-FILTER-PUSHDOWN-VIRTUAL-COLUMN-0",
        "reverse(countryIsoCode)",
        ValueType.STRING,
        ExprMacroTable.nil()
    );
    Assert.assertEquals(
        expectedFilterSplit.getBaseTableFilter(),
        actualFilterSplit.getBaseTableFilter()
    );
    Assert.assertEquals(
        expectedFilterSplit.getJoinTableFilter(),
        actualFilterSplit.getJoinTableFilter()
    );
    ExpressionVirtualColumn actualVirtualColumn = (ExpressionVirtualColumn) actualFilterSplit.getPushDownVirtualColumns()
                                                                                             .get(0);
    compareExpressionVirtualColumns(expectedVirtualColumn, actualVirtualColumn);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
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
            new Object[]{"Old Anatolian Turkish", "Ainigriv", "States United"}
        )
    );
  }

  @Test
  public void test_filterPushDown_factToRegionToCountryNotEquiJoinLeftFilterOnChannelAndCountryName()
  {
    JoinableClause factExprToRegon = new JoinableClause(
        FACT_TO_REGION_PREFIX,
        new IndexedTableJoinable(regionsTable),
        JoinType.LEFT,
        JoinConditionAnalysis.forExpression(
            StringUtils.format(
                "\"%sregionIsoCode\" == regionIsoCode && reverse(\"%scountryIsoCode\") == countryIsoCode",
                FACT_TO_REGION_PREFIX,
                FACT_TO_REGION_PREFIX
            ),
            FACT_TO_REGION_PREFIX,
            ExprMacroTable.nil()
        )
    );
    List<JoinableClause> joinableClauses = ImmutableList.of(
        factExprToRegon,
        regionToCountry(JoinType.LEFT)
    );

    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", "#en.wikipedia"),
            new SelectorFilter("rtc.countryName", "States United")
        )
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Cannot build hash-join matcher on non-equi-join condition: \"r1.regionIsoCode\" == regionIsoCode && reverse(\"r1.countryIsoCode\") == countryIsoCode");

    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );
    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
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
            new Object[]{"Old Anatolian Turkish", "Ainigriv", "States United"}
        )
    );
  }

  @Test
  public void test_filterPushDown_factToRegionToCountryLeftUnnormalizedFilter()
  {
    List<JoinableClause> joinableClauses = ImmutableList.of(
        factToRegion(JoinType.LEFT),
        regionToCountry(JoinType.LEFT)
    );
    Filter originalFilter = new OrFilter(
        ImmutableList.of(
            new SelectorFilter("channel", "#ko.wikipedia"),
            new AndFilter(
                ImmutableList.of(
                    new SelectorFilter("rtc.countryName", "United States"),
                    new SelectorFilter("r1.regionName", "Virginia")
                )
            )
        )
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new OrFilter(
                    ImmutableList.of(
                        new SelectorFilter("channel", "#ko.wikipedia"),
                        new InDimFilter("countryIsoCode", ImmutableSet.of("US"), null, null).toFilter()
                    )
                ),
                new OrFilter(
                    ImmutableList.of(
                        new SelectorFilter("channel", "#ko.wikipedia"),
                        new AndFilter(
                            ImmutableList.of(
                                new InDimFilter("countryIsoCode", ImmutableSet.of("US"), null, null).toFilter(),
                                new InDimFilter("regionIsoCode", ImmutableSet.of("VA"), null, null).toFilter()
                            )
                        )
                    )
                )
            )
        ),
        new AndFilter(
            ImmutableList.of(
                new OrFilter(
                    ImmutableList.of(
                        new SelectorFilter("channel", "#ko.wikipedia"),
                        new SelectorFilter("rtc.countryName", "United States")
                    )
                ),
                new OrFilter(
                    ImmutableList.of(
                        new SelectorFilter("channel", "#ko.wikipedia"),
                        new SelectorFilter("r1.regionName", "Virginia")
                    )
                )
            )
        ),
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
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
            new Object[]{"유희왕 GX", "Seoul", "Republic of Korea"},
            new Object[]{"Old Anatolian Turkish", "Virginia", "United States"}
        )
    );
  }

  @Test
  public void test_filterPushDown_factConcatExpressionToCountryLeftFilterOnChannelAndCountryName()
  {
    JoinableClause factExprToCountry = new JoinableClause(
        FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
        new IndexedTableJoinable(countriesTable),
        JoinType.LEFT,
        JoinConditionAnalysis.forExpression(
            StringUtils.format(
                "\"%scountryIsoCode\" == concat(countryIsoCode, regionIsoCode)",
                FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX
            ),
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
            ExprMacroTable.nil()
        )
    );
    List<JoinableClause> joinableClauses = ImmutableList.of(
        factExprToCountry
    );
    Filter filter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", "#en.wikipedia"),
            new SelectorFilter("c1.countryName", "Usca")
        )
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        filter
    );

    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    ExpressionVirtualColumn expectedVirtualColumn = new ExpressionVirtualColumn(
        "JOIN-FILTER-PUSHDOWN-VIRTUAL-COLUMN-0",
        "concat(countryIsoCode, regionIsoCode)",
        ValueType.STRING,
        ExprMacroTable.nil()
    );
    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", "#en.wikipedia"),
                new InDimFilter("JOIN-FILTER-PUSHDOWN-VIRTUAL-COLUMN-0", ImmutableSet.of("USCA"), null, null).toFilter()
            )
        ),
        new SelectorFilter("c1.countryName", "Usca"),
        ImmutableList.of(
            expectedVirtualColumn
        )
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(
        expectedFilterSplit.getBaseTableFilter(),
        actualFilterSplit.getBaseTableFilter()
    );
    Assert.assertEquals(
        expectedFilterSplit.getJoinTableFilter(),
        actualFilterSplit.getJoinTableFilter()
    );
    ExpressionVirtualColumn actualVirtualColumn = (ExpressionVirtualColumn) actualFilterSplit.getPushDownVirtualColumns()
                                                                                             .get(0);
    compareExpressionVirtualColumns(expectedVirtualColumn, actualVirtualColumn);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            filter,
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
            new Object[]{"President of India", "Usca"},
            new Object[]{"Otjiwarongo Airport", "Usca"},
            new Object[]{"Carlo Curti", "Usca"}
        )
    );
  }

  @Test
  public void test_filterPushDown_factToCountryRightWithFilterOnChannelAndJoinable()
  {
    List<JoinableClause> joinableClauses = ImmutableList.of(factToCountryOnIsoCode(JoinType.RIGHT));
    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", "#de.wikipedia"),
            new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName", "Germany")
        )
    );
    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", "#de.wikipedia"),
                new InDimFilter("countryIsoCode", ImmutableSet.of("DE"), null, null).toFilter()
            )
        ),
        new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName", "Germany"),
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
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
  public void test_filterPushDown_factToCountryRightWithFilterOnNullColumns()
  {
    List<JoinableClause> joinableClauses = ImmutableList.of(factToCountryOnIsoCode(JoinType.RIGHT));
    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", null),
            new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName", null)
        )
    );
    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        null,
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", null),
                new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName", null)
            )
        ),
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
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
        ImmutableList.of()
    );
  }

  @Test
  public void test_filterPushDown_factToCountryInnerUsingCountryNumberFilterOnChannelAndCountryName()
  {
    List<JoinableClause> joinableClauses = ImmutableList.of(factToCountryOnNumber(JoinType.INNER));
    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", "#en.wikipedia"),
            new SelectorFilter(FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "countryName", "Australia")
        )
    );
    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", "#en.wikipedia"),
                new InDimFilter("countryNumber", ImmutableSet.of("0"), null, null).toFilter()
            )
        ),
        new SelectorFilter(FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "countryName", "Australia"),
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    // In non-SQL-compatible mode, we get an extra row, since the 'null' countryNumber for "Talk:Oswald Tilghman"
    // is interpreted as 0 (a.k.a. Australia).
    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
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
            new Object[]{"Peremptory norm", "AU", "AU", "Australia", 0L}
        ) :
        ImmutableList.of(
            new Object[]{"Talk:Oswald Tilghman", null, "AU", "Australia", 0L},
            new Object[]{"Peremptory norm", "AU", "AU", "Australia", 0L}
        )
    );
  }

  @Test
  public void test_filterPushDown_factToCountryInnerUsingCountryNumberFilterOnNulls()
  {
    List<JoinableClause> joinableClauses = ImmutableList.of(factToCountryOnNumber(JoinType.INNER));
    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", null),
            new SelectorFilter(FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "countryName", null)
        )
    );
    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        null,
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", null),
                new SelectorFilter(FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "countryName", null)
            )
        ),
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
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
        ImmutableList.of()
    );
  }

  @Test
  public void test_filterPushDown_factToCountryFullWithFilterOnChannelAndCountryName()
  {
    Filter filter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", "#es.wikipedia"),
            new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName", "El Salvador")
        )
    );
    List<JoinableClause> joinableClauses = ImmutableList.of(factToCountryOnIsoCode(JoinType.FULL));
    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        filter
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", "#es.wikipedia"),
                new InDimFilter("countryIsoCode", ImmutableSet.of("SV"), null, null).toFilter()
            )
        ),
        new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName", "El Salvador"),
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            filter,
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
            new Object[]{"Wendigo", "SV", 12L, "SV", "El Salvador", 12L}
        )
    );
  }

  @Test
  public void test_filterPushDown_factToCountryFullWithFilterOnNulls()
  {
    List<JoinableClause> joinableClauses = ImmutableList.of(factToCountryOnIsoCode(JoinType.FULL));
    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", null),
            new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName", null)
        )
    );
    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        null,
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", null),
                new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName", null)
            )
        ),
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
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
        ImmutableList.of()
    );
  }

  @Test
  public void test_filterPushDown_factToRegionTwoColumnsToOneRHSColumnAndFilterOnRHS()
  {
    JoinableClause factExprToRegon = new JoinableClause(
        FACT_TO_REGION_PREFIX,
        new IndexedTableJoinable(regionsTable),
        JoinType.LEFT,
        JoinConditionAnalysis.forExpression(
            StringUtils.format(
                "\"%sregionIsoCode\" == regionIsoCode && \"%sregionIsoCode\" == countryIsoCode",
                FACT_TO_REGION_PREFIX,
                FACT_TO_REGION_PREFIX
            ),
            FACT_TO_REGION_PREFIX,
            ExprMacroTable.nil()
        )
    );
    List<JoinableClause> joinableClauses = ImmutableList.of(
        factExprToRegon
    );
    Filter originalFilter = new SelectorFilter("r1.regionName", "Fourems Province");

    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );

    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new InDimFilter("countryIsoCode", ImmutableSet.of("MMMM"), null, null).toFilter(),
                new InDimFilter("regionIsoCode", ImmutableSet.of("MMMM"), null, null).toFilter()
            )
        ),
        new SelectorFilter("r1.regionName", "Fourems Province"),
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            FACT_TO_REGION_PREFIX + "regionName"
        ),
        ImmutableList.of(
            new Object[]{"History of Fourems", "Fourems Province"}
        )
    );
  }

  @Test
  public void test_filterPushDown_factToRegionOneColumnToTwoRHSColumnsAndFilterOnRHS()
  {
    JoinableClause factExprToRegon = new JoinableClause(
        FACT_TO_REGION_PREFIX,
        new IndexedTableJoinable(regionsTable),
        JoinType.LEFT,
        JoinConditionAnalysis.forExpression(
            StringUtils.format(
                "\"%sregionIsoCode\" == regionIsoCode && \"%scountryIsoCode\" == regionIsoCode",
                FACT_TO_REGION_PREFIX,
                FACT_TO_REGION_PREFIX
            ),
            FACT_TO_REGION_PREFIX,
            ExprMacroTable.nil()
        )
    );
    List<JoinableClause> joinableClauses = ImmutableList.of(
        factExprToRegon
    );
    Filter originalFilter = new SelectorFilter("r1.regionName", "Fourems Province");

    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new InDimFilter("regionIsoCode", ImmutableSet.of("MMMM"), null, null).toFilter(),
        new SelectorFilter("r1.regionName", "Fourems Province"),
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        ),
        ImmutableList.of(
            "page",
            FACT_TO_REGION_PREFIX + "regionName"
        ),
        ImmutableList.of(
            new Object[]{"History of Fourems", "Fourems Province"}
        )
    );
  }

  @Test
  public void test_filterPushDown_factToRegionToCountryLeftFilterOnPageDisablePushDown()
  {
    List<JoinableClause> joinableClauses = ImmutableList.of(
        factToRegion(JoinType.LEFT),
        regionToCountry(JoinType.LEFT)
    );
    Filter originalFilter = new SelectorFilter("page", "Peremptory norm");

    JoinFilterPreAnalysis joinFilterPreAnalysis = JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
        joinableClauses,
        VirtualColumns.EMPTY,
        originalFilter,
        false,
        true,
        true,
        QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE_KEY
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        null,
        new SelectorFilter("page", "Peremptory norm"),
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
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
            new Object[]{"Peremptory norm", "New South Wales", "Australia"}
        )
    );
  }

  @Test
  public void test_filterPushDown_factToRegionToCountryLeftEnablePushDownDisableRewrite()
  {
    List<JoinableClause> joinableClauses = ImmutableList.of(
        factToRegion(JoinType.LEFT),
        regionToCountry(JoinType.LEFT)
    );
    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", "#en.wikipedia"),
            new SelectorFilter("rtc.countryName", "United States"),
            new OrFilter(
                ImmutableList.of(
                    new SelectorFilter("page", "DirecTV"),
                    new SelectorFilter("rtc.countryIsoCode", "US")
                )
            ),
            new BoundFilter(new BoundDimFilter(
                "namespace",
                "Main",
                "Main",
                false,
                false,
                null,
                null,
                null
            ))
        )
    );
    JoinFilterPreAnalysis joinFilterPreAnalysis = JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
        joinableClauses,
        VirtualColumns.EMPTY,
        originalFilter,
        true,
        false,
        true,
        QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE_KEY
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", "#en.wikipedia"),
                new BoundFilter(new BoundDimFilter(
                    "namespace",
                    "Main",
                    "Main",
                    false,
                    false,
                    null,
                    null,
                    null
                ))
            )
        ),
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("rtc.countryName", "United States"),
                new OrFilter(
                    ImmutableList.of(
                        new SelectorFilter("page", "DirecTV"),
                        new SelectorFilter("rtc.countryIsoCode", "US")
                    )
                )
            )
        ),
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
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
            new Object[]{"President of India", "California", "United States"},
            new Object[]{"Otjiwarongo Airport", "California", "United States"},
            new Object[]{"DirecTV", "North Carolina", "United States"},
            new Object[]{"Carlo Curti", "California", "United States"},
            new Object[]{"Old Anatolian Turkish", "Virginia", "United States"}
        )
    );
  }

  @Test
  public void test_filterPushDown_factToRegionToCountryLeftFilterOnRHSJoinConditionColumns()
  {
    Filter originalFilter = new SelectorFilter("rtc.countryIsoCode", "CA");
    List<JoinableClause> joinableClauses = ImmutableList.of(
        factToRegion(JoinType.LEFT),
        regionToCountry(JoinType.LEFT)
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );

    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new InDimFilter("countryIsoCode", ImmutableSet.of("CA"), null, null).toFilter(),
        new SelectorFilter("rtc.countryIsoCode", "CA"),
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
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
            new Object[]{"Didier Leclair", "Ontario", "Canada"},
            new Object[]{"Les Argonautes", "Quebec", "Canada"},
            new Object[]{"Sarah Michelle Gellar", "Ontario", "Canada"}
        )
    );
  }



  @Test
  public void test_filterPushDown_factToRegionToCountryLeftFilterOnTwoRHSColumnsSameValue()
  {
    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("r1.regionIsoCode", "CA"),
            new SelectorFilter("r1.countryIsoCode", "CA")
        )
    );

    List<JoinableClause> joinableClauses = ImmutableList.of(
        factToRegion(JoinType.LEFT),
        regionToCountry(JoinType.LEFT)
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = simplePreAnalysis(
        joinableClauses,
        originalFilter
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new AndFilter(
                    ImmutableList.of(
                        new InDimFilter("countryIsoCode", ImmutableSet.of("US"), null, null).toFilter(),
                        new InDimFilter("regionIsoCode", ImmutableSet.of("CA"), null, null).toFilter()
                    )
                ),
                new AndFilter(
                    ImmutableList.of(
                        new InDimFilter("countryIsoCode", ImmutableSet.of("CA"), null, null).toFilter(),
                        new InDimFilter("regionIsoCode", ImmutableSet.of("ON", "QC"), null, null).toFilter()
                    )
                )
            )
        ),
        originalFilter,
        ImmutableList.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);

    JoinTestHelper.verifyCursors(
        adapter.makeCursors(
            originalFilter,
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
        )
    );
  }

  @Test
  public void test_JoinFilterSplit_equals()
  {
    EqualsVerifier.forClass(JoinFilterSplit.class)
                  .usingGetClass()
                  .withNonnullFields("baseTableFilter", "pushDownVirtualColumns")
                  .verify();
  }

  private static JoinFilterPreAnalysis simplePreAnalysis(
      List<JoinableClause> joinableClauses,
      Filter originalFilter
  )
  {
    return JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
        joinableClauses,
        VirtualColumns.EMPTY,
        originalFilter,
        true,
        true,
        true,
        QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE_KEY
    );
  }
}
