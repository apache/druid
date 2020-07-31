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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.BoundFilter;
import org.apache.druid.segment.filter.FalseFilter;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.segment.join.filter.JoinFilterAnalyzer;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysisKey;
import org.apache.druid.segment.join.filter.JoinFilterSplit;
import org.apache.druid.segment.join.filter.JoinableClauses;
import org.apache.druid.segment.join.filter.rewrite.JoinFilterRewriteConfig;
import org.apache.druid.segment.join.lookup.LookupJoinable;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;

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

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );

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

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new SelectorFilter("channel", "#en.wikipedia"),
        null,
        ImmutableSet.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
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

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );

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
            new Object[]{"Cream Soda", "Ainigriv", "United States"}
        )
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        null,
        new SelectorFilter("rtc.countryName", "United States"),
        ImmutableSet.of()
    );
    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
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


    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );

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
            new Object[]{"President of India", "California", "United States"},
            new Object[]{"Otjiwarongo Airport", "California", "United States"},
            new Object[]{"DirecTV", "North Carolina", "United States"},
            new Object[]{"Carlo Curti", "California", "United States"},
            new Object[]{"Old Anatolian Turkish", "Virginia", "United States"}
        )
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", "#en.wikipedia"),
                new InDimFilter("countryIsoCode", ImmutableSet.of("US"), null, null).toFilter()
            )
        ),
        new SelectorFilter("rtc.countryName", "United States"),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
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

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );

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
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
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
            new SelectorFilter("baseTableInvalidColumn2", null),
            new SelectorFilter("rtc.invalidColumn", "abcd"),
            new SelectorFilter("r1.invalidColumn", "abcd")
        )
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );

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
        ImmutableList.of()
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new SelectorFilter("baseTableInvalidColumn", "abcd"),
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("baseTableInvalidColumn2", null),
                new SelectorFilter("rtc.invalidColumn", "abcd"),
                new SelectorFilter("r1.invalidColumn", "abcd")
            )
        ),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
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

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        virtualColumns
    );

    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

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

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new SelectorFilter("v1", "virtual-column-#en.wikipedia"),
        null,
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
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

    JoinableClauses joinableClauses = JoinableClauses.fromList(ImmutableList.of(
        factToRegion(JoinType.LEFT)
    ));

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses.getJoinableClauses(),
        virtualColumns
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses.getJoinableClauses(),
        joinFilterPreAnalysis
    );

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

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        null,
        new SelectorFilter("v0", "VIRGINIA"),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
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

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
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
            new Object[]{"Les Argonautes", "Quebec", "Canada"}
        )
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
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
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

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
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

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", "#en.wikipedia"),
                new InDimFilter("JOIN-FILTER-PUSHDOWN-VIRTUAL-COLUMN-0", ImmutableSet.of("SU"), null, null).toFilter()
            )
        ),
        new SelectorFilter("rtc.countryName", "States United"),
        ImmutableSet.of()
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
                                                                                             .iterator().next();
    compareExpressionVirtualColumns(expectedVirtualColumn, actualVirtualColumn);
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

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Cannot build hash-join matcher on non-equi-join condition: \"r1.regionIsoCode\" == regionIsoCode && reverse(\"r1.countryIsoCode\") == countryIsoCode");

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

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
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
            new Object[]{"유희왕 GX", "Seoul", "Republic of Korea"},
            new Object[]{"Old Anatolian Turkish", "Virginia", "United States"}
        )
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
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
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

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        filter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

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
        ImmutableSet.of(
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
                                                                                             .iterator().next();
    compareExpressionVirtualColumns(expectedVirtualColumn, actualVirtualColumn);
  }

  @Test
  public void test_filterPushDown_factConcatExpressionToCountryLeftFilterOnChannelAndCountryNameUsingLookup()
  {
    JoinableClause factExprToCountry = new JoinableClause(
        FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX,
        LookupJoinable.wrap(countryIsoCodeToNameLookup),
        JoinType.LEFT,
        JoinConditionAnalysis.forExpression(
            StringUtils.format(
                "\"%sk\" == concat(countryIsoCode, regionIsoCode)",
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
            new SelectorFilter("c1.v", "Usca")
        )
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        filter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

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
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "v"
        ),
        ImmutableList.of(
            new Object[]{"President of India", "Usca"},
            new Object[]{"Otjiwarongo Airport", "Usca"},
            new Object[]{"Carlo Curti", "Usca"}
        )
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
        new SelectorFilter("c1.v", "Usca"),
        ImmutableSet.of(
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
                                                                                             .iterator().next();
    compareExpressionVirtualColumns(expectedVirtualColumn, actualVirtualColumn);
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
    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
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

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", "#de.wikipedia"),
                new InDimFilter("countryIsoCode", ImmutableSet.of("DE"), null, null).toFilter()
            )
        ),
        new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName", "Germany"),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
  }

  @Test
  public void test_filterPushDown_factToCountryRightWithFilterOnChannelAndJoinableUsingLookup()
  {
    List<JoinableClause> joinableClauses = ImmutableList.of(factToCountryNameUsingIsoCodeLookup(JoinType.RIGHT));
    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", "#de.wikipedia"),
            new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "v", "Germany")
        )
    );
    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
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
            "countryIsoCode",
            "countryNumber",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "k",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "v"
        ),
        ImmutableList.of(
            new Object[]{"Diskussion:Sebastian Schulz", "DE", 3L, "DE", "Germany"}
        )
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", "#de.wikipedia"),
                new InDimFilter("countryIsoCode", ImmutableSet.of("DE"), null, null).toFilter()
            )
        ),
        new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "v", "Germany"),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
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
    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
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
            "countryIsoCode",
            "countryNumber",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryNumber"
        ),
        ImmutableList.of()
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        null,
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", null),
                new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName", null)
            )
        ),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
  }

  @Test
  public void test_filterPushDown_factToCountryRightWithFilterOnValueThatMatchesNothing()
  {
    List<JoinableClause> joinableClauses = ImmutableList.of(factToCountryOnIsoCode(JoinType.RIGHT));
    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", null),
            new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName", "NO MATCH")
        )
    );
    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
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
            "countryIsoCode",
            "countryNumber",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryNumber"
        ),
        ImmutableList.of()
    );


    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        FalseFilter.instance(),
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", null),
                new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName", "NO MATCH")
            )
        ),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
  }

  @Test
  public void test_filterPushDown_factToCountryRightWithFilterOnNullColumnsUsingLookup()
  {
    List<JoinableClause> joinableClauses = ImmutableList.of(factToCountryNameUsingIsoCodeLookup(JoinType.RIGHT));
    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", null),
            new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "v", null)
        )
    );
    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
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
            "countryIsoCode",
            "countryNumber",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "k",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "v"
        ),
        ImmutableList.of()
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        null,
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", null),
                new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "v", null)
            )
        ),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
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
    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

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

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", "#en.wikipedia"),
                new InDimFilter("countryNumber", ImmutableSet.of("0"), null, null).toFilter()
            )
        ),
        new SelectorFilter(FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "countryName", "Australia"),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
  }

  @Test
  public void test_filterPushDown_factToCountryInnerUsingCountryNumberFilterOnChannelAndCountryNameUsingLookup()
  {
    List<JoinableClause> joinableClauses = ImmutableList.of(factToCountryNameUsingNumberLookup(JoinType.INNER));
    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", "#en.wikipedia"),
            new SelectorFilter(FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "v", "Australia")
        )
    );
    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

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
            FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "k",
            FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "v"
        ),
        NullHandling.sqlCompatible() ?
        ImmutableList.of(
            new Object[]{"Peremptory norm", "AU", "0", "Australia"}
        ) :
        ImmutableList.of(
            new Object[]{"Talk:Oswald Tilghman", null, "0", "Australia"},
            new Object[]{"Peremptory norm", "AU", "0", "Australia"}
        )
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", "#en.wikipedia"),
                new InDimFilter("countryNumber", ImmutableSet.of("0"), null, null).toFilter()
            )
        ),
        new SelectorFilter(FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "v", "Australia"),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
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
    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
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
            "countryIsoCode",
            FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "countryIsoCode",
            FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "countryName",
            FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "countryNumber"
        ),
        ImmutableList.of()
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        null,
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", null),
                new SelectorFilter(FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "countryName", null)
            )
        ),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
  }

  @Test
  public void test_filterPushDown_factToCountryInnerUsingCountryNumberFilterOnNullsUsingLookup()
  {
    List<JoinableClause> joinableClauses = ImmutableList.of(factToCountryNameUsingIsoCodeLookup(JoinType.INNER));
    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", null),
            new SelectorFilter(FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "v", null)
        )
    );
    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
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
            "countryIsoCode",
            FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "k",
            FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "v"
        ),
        ImmutableList.of()
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        null,
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", null),
                new SelectorFilter(FACT_TO_COUNTRY_ON_NUMBER_PREFIX + "v", null)
            )
        ),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
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
    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        filter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

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

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", "#es.wikipedia"),
                new InDimFilter("countryIsoCode", ImmutableSet.of("SV"), null, null).toFilter()
            )
        ),
        new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName", "El Salvador"),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
  }

  @Test
  public void test_filterPushDown_factToCountryFullWithFilterOnChannelAndCountryNameUsingLookup()
  {
    Filter filter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", "#es.wikipedia"),
            new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "v", "El Salvador")
        )
    );
    List<JoinableClause> joinableClauses = ImmutableList.of(factToCountryNameUsingIsoCodeLookup(JoinType.FULL));
    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        filter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

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
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "k",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "v"
        ),
        ImmutableList.of(
            new Object[]{"Wendigo", "SV", 12L, "SV", "El Salvador"}
        )
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", "#es.wikipedia"),
                new InDimFilter("countryIsoCode", ImmutableSet.of("SV"), null, null).toFilter()
            )
        ),
        new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "v", "El Salvador"),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
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
    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
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
            "countryIsoCode",
            "countryNumber",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryIsoCode",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryNumber"
        ),
        ImmutableList.of()
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        null,
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", null),
                new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "countryName", null)
            )
        ),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
  }

  @Test
  public void test_filterPushDown_factToCountryFullWithFilterOnNullsUsingLookup()
  {
    List<JoinableClause> joinableClauses = ImmutableList.of(factToCountryNameUsingIsoCodeLookup(JoinType.FULL));
    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("channel", null),
            new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "v", null)
        )
    );
    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
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
            "countryIsoCode",
            "countryNumber",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "k",
            FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "v"
        ),
        ImmutableList.of()
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        null,
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter("channel", null),
                new SelectorFilter(FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX + "v", null)
            )
        ),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
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

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
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
            FACT_TO_REGION_PREFIX + "regionName"
        ),
        ImmutableList.of(
            new Object[]{"History of Fourems", "Fourems Province"}
        )
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new InDimFilter("countryIsoCode", ImmutableSet.of("MMMM"), null, null).toFilter(),
                new InDimFilter("regionIsoCode", ImmutableSet.of("MMMM"), null, null).toFilter()
            )
        ),
        new SelectorFilter("r1.regionName", "Fourems Province"),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
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
    Filter originalFilter = new OrFilter(
        ImmutableList.of(
            new SelectorFilter("r1.regionName", "Fourems Province"),
            new SelectorFilter("r1.regionIsoCode", "AAAA")
        )
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
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
            FACT_TO_REGION_PREFIX + "regionName"
        ),
        ImmutableList.of(
            new Object[]{"History of Fourems", "Fourems Province"}
        )
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new OrFilter(
            ImmutableList.of(
                new InDimFilter("regionIsoCode", ImmutableSet.of("MMMM"), null, null).toFilter(),
                new SelectorFilter("regionIsoCode", "AAAA")
            )
        ),
        originalFilter,
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
  }


  @Test
  public void test_filterPushDown_factToRegionThreeRHSColumnsAllDirectAndFilterOnRHS()
  {
    JoinableClause factExprToRegon = new JoinableClause(
        FACT_TO_REGION_PREFIX,
        new IndexedTableJoinable(regionsTable),
        JoinType.LEFT,
        JoinConditionAnalysis.forExpression(
            StringUtils.format(
                "\"%sregionIsoCode\" == regionIsoCode && \"%scountryIsoCode\" == regionIsoCode && \"%sregionName\" == user",
                FACT_TO_REGION_PREFIX,
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
    Filter originalFilter = new OrFilter(
        ImmutableList.of(
            new SelectorFilter("r1.regionName", "Fourems Province"),
            new SelectorFilter("r1.regionIsoCode", "AAAA")
        )
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    // This query doesn't execute because regionName is not a key column, but we can still check the
    // filter rewrites.
    expectedException.expect(IAE.class);
    expectedException.expectMessage(
        "Cannot build hash-join matcher on non-key-based condition: Equality{leftExpr=user, rightColumn='regionName'}"
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
            FACT_TO_REGION_PREFIX + "regionName"
        ),
        ImmutableList.of()
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new OrFilter(
            ImmutableList.of(
                new SelectorFilter("user", "Fourems Province"),
                new SelectorFilter("regionIsoCode", "AAAA")
            )
        ),
        null,
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
  }

  @Test
  public void test_filterPushDown_factToRegionToCountryLeftFilterOnPageDisablePushDown()
  {
    JoinableClauses joinableClauses = JoinableClauses.fromList(ImmutableList.of(
        factToRegion(JoinType.LEFT),
        regionToCountry(JoinType.LEFT)
    ));
    Filter originalFilter = new SelectorFilter("page", "Peremptory norm");

    JoinFilterPreAnalysis joinFilterPreAnalysis = JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
        new JoinFilterPreAnalysisKey(
            new JoinFilterRewriteConfig(
                false,
                true,
                true,
                QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE
            ),
            joinableClauses.getJoinableClauses(),
            VirtualColumns.EMPTY,
            originalFilter
        )
    );

    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses.getJoinableClauses(),
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
            new Object[]{"Peremptory norm", "New South Wales", "Australia"}
        )
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        null,
        new SelectorFilter("page", "Peremptory norm"),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
  }

  @Test
  public void test_filterPushDown_factToRegionToCountryLeftEnablePushDownDisableRewrite()
  {
    JoinableClauses joinableClauses = JoinableClauses.fromList(ImmutableList.of(
        factToRegion(JoinType.LEFT),
        regionToCountry(JoinType.LEFT)
    ));
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
        new JoinFilterPreAnalysisKey(
            new JoinFilterRewriteConfig(
                true,
                false,
                true,
                QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE
            ),
            joinableClauses.getJoinableClauses(),
            VirtualColumns.EMPTY,
            originalFilter
        )
    );

    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses.getJoinableClauses(),
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
            new Object[]{"President of India", "California", "United States"},
            new Object[]{"Otjiwarongo Airport", "California", "United States"},
            new Object[]{"DirecTV", "North Carolina", "United States"},
            new Object[]{"Carlo Curti", "California", "United States"},
            new Object[]{"Old Anatolian Turkish", "Virginia", "United States"}
        )
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
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
  }

  @Test
  public void test_filterPushDown_factToRegionToCountryLeftFilterOnRHSJoinConditionColumns()
  {
    test_filterPushDown_factToRegionToCountryLeftFilterOnRHSJoinConditionColumnsHelper(false);
  }

  @Test
  public void test_filterPushDown_factToRegionToCountryLeftFilterOnRHSJoinConditionColumnsWithLhsExpr()
  {
    test_filterPushDown_factToRegionToCountryLeftFilterOnRHSJoinConditionColumnsHelper(true);
  }

  private void test_filterPushDown_factToRegionToCountryLeftFilterOnRHSJoinConditionColumnsHelper(boolean hasLhsExpressionInJoinCondition)
  {
    Filter expressionFilter = new ExpressionDimFilter(
        "\"rtc.countryIsoCode\" == 'CA'",
        ExprMacroTable.nil()
    ).toFilter();

    Filter specialSelectorFilter = new SelectorFilter("rtc.countryIsoCode", "CA")
    {
      @Override
      public boolean supportsRequiredColumnRewrite()
      {
        return false;
      }
    };

    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("r1.regionIsoCode", "ON"),
            new SelectorFilter("rtc.countryIsoCode", "CA"),
            specialSelectorFilter,
            new BoundFilter(new BoundDimFilter(
                "rtc.countryIsoCode",
                "CA",
                "CB",
                false,
                false,
                null,
                null,
                null
            )),
            expressionFilter,
            new InDimFilter("rtc.countryIsoCode", ImmutableSet.of("CA", "CA2", "CA3"), null, null).toFilter(),
            new OrFilter(
                ImmutableList.of(
                    new SelectorFilter("channel", "#fr.wikipedia"),
                    new SelectorFilter("rtc.countryIsoCode", "QQQ"),
                    new BoundFilter(new BoundDimFilter(
                        "rtc.countryIsoCode",
                        "YYY",
                        "ZZZ",
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
                    new SelectorFilter("namespace", "Main"),
                    new SelectorFilter("rtc.countryIsoCode", "ABCDEF"),
                    new SelectorFilter("rtc.countryName", "Canada"),
                    new BoundFilter(new BoundDimFilter(
                        "rtc.countryIsoCode",
                        "XYZXYZ",
                        "XYZXYZ",
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

    JoinableClause factToRegionClause;
    if (hasLhsExpressionInJoinCondition) {
      factToRegionClause = new JoinableClause(
          FACT_TO_REGION_PREFIX,
          new IndexedTableJoinable(regionsTable),
          JoinType.LEFT,
          JoinConditionAnalysis.forExpression(
              StringUtils.format(
                  "\"%sregionIsoCode\" == upper(lower(regionIsoCode)) && \"%scountryIsoCode\" == upper(lower(countryIsoCode))",
                  FACT_TO_REGION_PREFIX,
                  FACT_TO_REGION_PREFIX
              ),
              FACT_TO_REGION_PREFIX,
              ExprMacroTable.nil()
          )
      );
    } else {
      factToRegionClause = factToRegion(JoinType.LEFT);
    }

    List<JoinableClause> joinableClauses = ImmutableList.of(
        factToRegionClause,
        regionToCountry(JoinType.LEFT)
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );
    HashJoinSegmentStorageAdapter adapter = new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    String rewrittenCountryIsoCodeColumnName = hasLhsExpressionInJoinCondition
                                               ? "JOIN-FILTER-PUSHDOWN-VIRTUAL-COLUMN-0"
                                               : "countryIsoCode";


    String rewrittenRegionIsoCodeColumnName = hasLhsExpressionInJoinCondition
                                              ? "JOIN-FILTER-PUSHDOWN-VIRTUAL-COLUMN-1"
                                              : "regionIsoCode";

    Set<VirtualColumn> expectedVirtualColumns;
    if (hasLhsExpressionInJoinCondition) {
      expectedVirtualColumns = ImmutableSet.of(
          new ExpressionVirtualColumn(
              rewrittenRegionIsoCodeColumnName,
              "(upper [(lower [regionIsoCode])])",
              ValueType.STRING,
              ExprMacroTable.nil()
          ),
          new ExpressionVirtualColumn(
              rewrittenCountryIsoCodeColumnName,
              "(upper [(lower [countryIsoCode])])",
              ValueType.STRING,
              ExprMacroTable.nil()
          )
      );
    } else {
      expectedVirtualColumns = ImmutableSet.of();
    }

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
            new Object[]{"Didier Leclair", "Ontario", "Canada"}
        )
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new SelectorFilter(rewrittenRegionIsoCodeColumnName, "ON"),
                new SelectorFilter(rewrittenCountryIsoCodeColumnName, "CA"),
                new BoundFilter(new BoundDimFilter(
                    rewrittenCountryIsoCodeColumnName,
                    "CA",
                    "CB",
                    false,
                    false,
                    null,
                    null,
                    null
                )),
                new InDimFilter(
                    rewrittenCountryIsoCodeColumnName,
                    ImmutableSet.of("CA", "CA2", "CA3"),
                    null,
                    null
                ).toFilter(),
                new InDimFilter(rewrittenCountryIsoCodeColumnName, ImmutableSet.of("CA"), null, null).toFilter(),
                new OrFilter(
                    ImmutableList.of(
                        new SelectorFilter("channel", "#fr.wikipedia"),
                        new SelectorFilter(rewrittenCountryIsoCodeColumnName, "QQQ"),
                        new BoundFilter(new BoundDimFilter(
                            rewrittenCountryIsoCodeColumnName,
                            "YYY",
                            "ZZZ",
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
                        new SelectorFilter("namespace", "Main"),
                        new SelectorFilter(rewrittenCountryIsoCodeColumnName, "ABCDEF"),
                        new InDimFilter(
                            rewrittenCountryIsoCodeColumnName,
                            ImmutableSet.of("CA"),
                            null,
                            null
                        ).toFilter(),
                        new BoundFilter(new BoundDimFilter(
                            rewrittenCountryIsoCodeColumnName,
                            "XYZXYZ",
                            "XYZXYZ",
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
        new AndFilter(
            ImmutableList.of(
                specialSelectorFilter,
                expressionFilter,
                new OrFilter(
                    ImmutableList.of(
                        new SelectorFilter("namespace", "Main"),
                        new SelectorFilter("rtc.countryIsoCode", "ABCDEF"),
                        new SelectorFilter("rtc.countryName", "Canada"),
                        new BoundFilter(new BoundDimFilter(
                            "rtc.countryIsoCode",
                            "XYZXYZ",
                            "XYZXYZ",
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
        expectedVirtualColumns
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
  }

  @Test
  public void test_filterPushDown_factToRegionToCountryLeftFilterOnTwoRHSColumnsSameValue()
  {
    Filter originalFilter = new AndFilter(
        ImmutableList.of(
            new SelectorFilter("r1.regionName", "California"),
            new SelectorFilter("r1.extraField", "California")
        )
    );

    List<JoinableClause> joinableClauses = ImmutableList.of(
        factToRegion(JoinType.LEFT),
        regionToCountry(JoinType.LEFT)
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        originalFilter,
        joinableClauses,
        VirtualColumns.EMPTY
    );

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
        )
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        new AndFilter(
            ImmutableList.of(
                new AndFilter(
                    ImmutableList.of(
                        new InDimFilter("countryIsoCode", ImmutableSet.of("MMMM", "AAAA"), null, null).toFilter(),
                        new InDimFilter("regionIsoCode", ImmutableSet.of("MMMM", "AAAA"), null, null).toFilter()
                    )
                ),
                new AndFilter(
                    ImmutableList.of(
                        new InDimFilter("countryIsoCode", ImmutableSet.of("US"), null, null).toFilter(),
                        new InDimFilter("regionIsoCode", ImmutableSet.of("CA"), null, null).toFilter()
                    )
                )
            )
        ),
        originalFilter,
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
  }


  @Test
  public void test_filterPushDown_factToRegionExprToCountryLeftFilterOnCountryNameWithMultiLevelMode()
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

    JoinFilterPreAnalysis joinFilterPreAnalysis = JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
        new JoinFilterPreAnalysisKey(
            new JoinFilterRewriteConfig(
                true,
                true,
                true,
                QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE
            ),
            joinableClauses,
            VirtualColumns.EMPTY,
            originalFilter
        )
    );

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
            new Object[]{"Cream Soda", "Ainigriv", "United States"}
        )
    );

    JoinFilterSplit expectedFilterSplit = new JoinFilterSplit(
        null,
        new SelectorFilter("rtc.countryName", "United States"),
        ImmutableSet.of()
    );

    JoinFilterSplit actualFilterSplit = JoinFilterAnalyzer.splitFilter(joinFilterPreAnalysis);
    Assert.assertEquals(expectedFilterSplit, actualFilterSplit);
  }

  @Test
  public void test_JoinFilterSplit_equals()
  {
    EqualsVerifier.forClass(JoinFilterSplit.class)
                  .usingGetClass()
                  .withNonnullFields("baseTableFilter", "pushDownVirtualColumns")
                  .verify();
  }


  @Test
  public void test_joinFilterPreAnalysisKey_equals()
  {
    EqualsVerifier.forClass(JoinFilterPreAnalysisKey.class)
                  .usingGetClass()
                  .withNonnullFields("virtualColumns")
                  .verify();
  }
}
