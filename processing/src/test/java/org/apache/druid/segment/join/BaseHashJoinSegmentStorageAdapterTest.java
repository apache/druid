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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.join.filter.JoinFilterAnalyzer;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysisKey;
import org.apache.druid.segment.join.filter.rewrite.JoinFilterRewriteConfig;
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
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;

public class BaseHashJoinSegmentStorageAdapterTest
{
  public static JoinFilterRewriteConfig DEFAULT_JOIN_FILTER_REWRITE_CONFIG = new JoinFilterRewriteConfig(
      true,
      true,
      true,
      QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE
  );

  public static final String FACT_TO_COUNTRY_ON_ISO_CODE_PREFIX = "c1.";
  public static final String FACT_TO_COUNTRY_ON_NUMBER_PREFIX = "c2.";
  public static final String FACT_TO_REGION_PREFIX = "r1.";
  public static final String REGION_TO_COUNTRY_PREFIX = "rtc.";
  public static Long NULL_COUNTRY;

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

  protected JoinableClause factToCountryNameUsingIsoCodeLookup(final JoinType joinType)
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

  protected JoinableClause factToCountryNameUsingNumberLookup(final JoinType joinType)
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

  protected JoinableClause factToCountryOnIsoCode(final JoinType joinType)
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

  protected JoinableClause factToCountryOnNumber(final JoinType joinType)
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

  protected JoinableClause factToRegion(final JoinType joinType)
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

  protected JoinableClause regionToCountry(final JoinType joinType)
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

  /**
   * Creates a fact-to-country join segment without a {@link JoinFilterPreAnalysis}. This means it cannot
   * have {@link org.apache.druid.segment.StorageAdapter#makeCursors} called on it.
   */
  protected HashJoinSegmentStorageAdapter makeFactToCountrySegment()
  {
    return new HashJoinSegmentStorageAdapter(
        factSegment.asStorageAdapter(),
        ImmutableList.of(factToCountryOnIsoCode(JoinType.LEFT)),
        null
    );
  }

  protected void compareExpressionVirtualColumns(
      ExpressionVirtualColumn expectedVirtualColumn,
      ExpressionVirtualColumn actualVirtualColumn
  )
  {
    Assert.assertEquals(
        expectedVirtualColumn.getOutputName(),
        actualVirtualColumn.getOutputName()
    );
    Assert.assertEquals(
        expectedVirtualColumn.getOutputType(),
        actualVirtualColumn.getOutputType()
    );
    Assert.assertEquals(
        expectedVirtualColumn.getParsedExpression().get().toString(),
        actualVirtualColumn.getParsedExpression().get().toString()
    );
  }

  protected static JoinFilterPreAnalysis makeDefaultConfigPreAnalysis(
      Filter originalFilter,
      List<JoinableClause> joinableClauses,
      VirtualColumns virtualColumns
  )
  {
    return JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
        new JoinFilterPreAnalysisKey(
            DEFAULT_JOIN_FILTER_REWRITE_CONFIG,
            joinableClauses,
            virtualColumns,
            originalFilter
        )
    );
  }
}
