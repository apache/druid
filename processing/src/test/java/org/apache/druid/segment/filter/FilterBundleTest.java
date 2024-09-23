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

import com.google.common.collect.ImmutableList;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterBundle;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.ColumnCache;
import org.apache.druid.segment.ColumnSelectorColumnIndexSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class FilterBundleTest extends InitializedNullHandlingTest
{
  private Closer closer;
  protected BitmapFactory bitmapFactory;
  protected ColumnIndexSelector indexSelector;

  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder();

  @Before
  public void setUp()
  {
    final QueryableIndex index = TestIndex.getMMappedWikipediaIndex();
    closer = Closer.create();
    bitmapFactory = index.getBitmapFactoryForDimensions();
    indexSelector = new ColumnSelectorColumnIndexSelector(
        bitmapFactory,
        VirtualColumns.EMPTY,
        new ColumnCache(index, closer)
    );
  }

  @After
  public void tearDown() throws Exception
  {
    closer.close();
    indexSelector = null;
  }

  @Test
  @Parameters({"true", "false"})
  public void test_or_country_isRobot(boolean cursorAutoArrangeFilters)
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new OrFilter(
            ImmutableList.of(
                new EqualityFilter("countryName", ColumnType.STRING, "United States", null),
                new EqualityFilter("isRobot", ColumnType.STRING, "true", null)
            )
        ),
        cursorAutoArrangeFilters
    );

    Assert.assertEquals(
        "index: OR (selectionSize = 39244)\n"
        + "  index: countryName = United States (selectionSize = 528)\n"
        + "  index: isRobot = true (selectionSize = 15420)\n",
        filterBundle.getInfo().describe()
    );
  }

  @Test
  @Parameters({"true", "false"})
  public void test_and_country_isRobot(boolean cursorAutoArrangeFilters)
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new AndFilter(
            ImmutableList.of(
                new EqualityFilter("countryName", ColumnType.STRING, "United States", null),
                new EqualityFilter("isRobot", ColumnType.STRING, "true", null)
            )
        ),
        cursorAutoArrangeFilters
    );

    Assert.assertEquals(
        "index: AND (selectionSize = 0)\n"
        + "  index: countryName = United States (selectionSize = 528)\n"
        + "  index: isRobot = true (selectionSize = 15420)\n",
        filterBundle.getInfo().describe()
    );
  }

  @Test
  @Parameters({"true", "false"})
  public void test_or_countryIsNull_pageLike(boolean cursorAutoArrangeFilters)
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new OrFilter(
            ImmutableList.of(
                new NullFilter("countryName", null),
                new LikeDimFilter("page", "%u%", null, null).toFilter()
            )
        ),
        cursorAutoArrangeFilters
    );

    Assert.assertEquals(
        "matcher: OR\n"
        + "  matcher: countryName IS NULL\n"
        + "    with partial index: countryName IS NULL (selectionSize = 35445)\n"
        + "  matcher: page LIKE '%u%'\n",
        filterBundle.getInfo().describe()
    );
  }

  @Test
  @Parameters({"true", "false"})
  public void test_and_countryIsNull_pageLike(boolean cursorAutoArrangeFilters)
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new AndFilter(
            ImmutableList.of(
                new NullFilter("countryName", null),
                new LikeDimFilter("page", "%u%", null, null).toFilter()
            )
        ),
        cursorAutoArrangeFilters
    );

    Assert.assertEquals(
        "index: AND (selectionSize = 14165)\n"
        + "  index: countryName IS NULL (selectionSize = 35445)\n"
        + "  index: page LIKE '%u%' (selectionSize = 15328)\n",
        filterBundle.getInfo().describe()
    );
  }

  @Test
  @Parameters({"true", "false"})
  public void test_and_country_pageLike(boolean cursorAutoArrangeFilters)
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new AndFilter(
            ImmutableList.of(
                new EqualityFilter("countryName", ColumnType.STRING, "United States", null),
                new LikeDimFilter("page", "%u%", null, null).toFilter()
            )
        ),
        cursorAutoArrangeFilters
    );

    Assert.assertEquals(
        "index: countryName = United States (selectionSize = 528)\n"
        + "matcher: page LIKE '%u%'\n",
        filterBundle.getInfo().describe()
    );
  }

  @Test
  @Parameters({"true"})
  public void test_pageLike_and_country_pageLike_with_cursorAutoArrangeFilters(boolean cursorAutoArrangeFilters)
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new AndFilter(
            ImmutableList.of(
                new LikeDimFilter("page", "%u%", null, null).toFilter(),
                new EqualityFilter("countryName", ColumnType.STRING, "United States", null)
            )
        ),
        cursorAutoArrangeFilters
    );

    // With cursorAutoArrangeFilters flag on, the indexes are sorted by cost ASC, hence country name index is used first.
    Assert.assertEquals(
        "index: countryName = United States (selectionSize = 528)\n"
        + "matcher: page LIKE '%u%'\n",
        filterBundle.getInfo().describe()
    );
  }

  @Test
  @Parameters({"true", "false"})
  public void test_or_countryNotNull_pageLike(boolean cursorAutoArrangeFilters)
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new OrFilter(
            ImmutableList.of(
                new NotFilter(new NullFilter("countryName", null)),
                new LikeDimFilter("page", "%u%", null, null).toFilter()
            )
        ),
        cursorAutoArrangeFilters
    );

    Assert.assertEquals(
        "index: OR (selectionSize = 39244)\n"
        + "  index: ~(countryName IS NULL) (selectionSize = 3799)\n"
        + "  index: page LIKE '%u%' (selectionSize = 15328)\n",
        filterBundle.getInfo().describe()
    );
  }

  @Test
  @Parameters({"true"})
  public void test_or_pageLike_countryNotNull_pageLike_with_cursorAutoArrangeFilters(boolean cursorAutoArrangeFilters)
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new OrFilter(
            ImmutableList.of(
                new LikeDimFilter("page", "%u%", null, null).toFilter(),
                new NotFilter(new NullFilter("countryName", null))
            )
        ),
        cursorAutoArrangeFilters
    );

    // With cursorAutoArrangeFilters flag on, the indexes are sorted by cost ASC, hence country name index is used first.
    Assert.assertEquals(
        "index: OR (selectionSize = 39244)\n"
        + "  index: ~(countryName IS NULL) (selectionSize = 3799)\n"
        + "  index: page LIKE '%u%' (selectionSize = 15328)\n",
        filterBundle.getInfo().describe()
    );
  }

  @Test
  @Parameters({"true", "false"})
  public void test_and_countryNotNull_pageLike(boolean cursorAutoArrangeFilters)
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new AndFilter(
            ImmutableList.of(
                new NotFilter(new NullFilter("countryName", null)),
                new LikeDimFilter("page", "%u%", null, null).toFilter()
            )
        ),
        cursorAutoArrangeFilters
    );

    Assert.assertEquals(
        "index: ~(countryName IS NULL) (selectionSize = 3799)\n"
        + "matcher: page LIKE '%u%'\n",
        filterBundle.getInfo().describe()
    );
  }


  @Test
  @Parameters({"true"})
  public void test_and_cursorAutoArrangeFilters(boolean cursorAutoArrangeFilters)
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new AndFilter(
            ImmutableList.of(
                new OrFilter(
                    ImmutableList.of(
                        new LikeDimFilter("page", "O%", null, null).toFilter(),
                        new EqualityFilter("countryName", ColumnType.STRING, "United States", null)
                    )
                ),
                new EqualityFilter("isRobot", ColumnType.STRING, "false", null),
                new NotFilter(new NullFilter("countryName", null))
            )
        ),
        cursorAutoArrangeFilters
    );

    // The estimate cost for child Filters:
    // 1. countryName NullFilter: 0
    // 2. isRobot EqualityFilter: 1
    // 3. page LikeFilter with prefix and countryName EqualityFIlter: 1 + size of page dictionary with keys matching "O" prefix
    Assert.assertEquals(
        "index: AND (selectionSize = 562)\n"
        + "  index: ~(countryName IS NULL) (selectionSize = 3799)\n"
        + "  index: isRobot = false (selectionSize = 23824)\n"
        + "  index: OR (selectionSize = 3799)\n"
        + "    index: countryName = United States (selectionSize = 528)\n"
        + "    index: page LIKE 'O%' (selectionSize = 351)\n",
        filterBundle.getInfo().describe()
    );
  }

  @Test
  @Parameters({"true"})
  public void test_or_cursorAutoArrangeFilters(boolean cursorAutoArrangeFilters)
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new OrFilter(
            ImmutableList.of(
                new AndFilter(
                    ImmutableList.of(
                        new EqualityFilter("countryName", ColumnType.STRING, "United Kingdom", null),
                        new LikeDimFilter("page", "%b%", null, null).toFilter()
                    )
                ),
                new AndFilter(
                    ImmutableList.of(
                        new EqualityFilter("countryName", ColumnType.STRING, "United States", null),
                        new LikeDimFilter("page", "O%", null, null).toFilter()
                    )
                ),
                new TypedInFilter("isRobot", ColumnType.STRING, ImmutableList.of("false", "true"), null, null),
                new EqualityFilter("channel", ColumnType.STRING, "#en.wikipedia", null),
                new NullFilter("countryName", null)
            )
        ),
        cursorAutoArrangeFilters
    );

    // The estimate cost for child Filters:
    // 1. countryName NullFilter: 0
    // 2. channel EqualityFilter: 1
    // 3. isRobot TypedInFilter: 2
    // 4. page LikeFilter with prefix and countryName EqualityFilter: 1 + size of page dictionary with keys matching "O" prefix
    // 5. page LikeFilter and countryName EqualityFilter: 1 + size of page dictionary
    Assert.assertEquals(
        "matcher: OR\n"
        + "  matcher: OR\n"
        + "    with partial index: OR (selectionSize = 39244)\n"
        + "      index: countryName IS NULL (selectionSize = 35445)\n"
        + "      index: channel = #en.wikipedia (selectionSize = 11549)\n"
        + "      index: isRobot IN (false, true) (STRING) (selectionSize = 39244)\n"
        + "  matcher: AND\n"
        + "    with partial index: countryName = United States (selectionSize = 528)\n"
        + "    matcher: page LIKE 'O%'\n"
        + "  matcher: AND\n"
        + "    with partial index: countryName = United Kingdom (selectionSize = 234)\n"
        + "    matcher: page LIKE '%b%'\n",
        filterBundle.getInfo().describe()
    );

  }

  @Test
  @Parameters({"true", "false"})
  public void test_or_countryIsAndPageLike(boolean cursorAutoArrangeFilters)
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new OrFilter(
            ImmutableList.of(
                new AndFilter(
                    ImmutableList.of(
                        new EqualityFilter("countryName", ColumnType.STRING, "United States", null),
                        new LikeDimFilter("page", "%a%", null, null).toFilter()
                    )
                ),
                new AndFilter(
                    ImmutableList.of(
                        new EqualityFilter("countryName", ColumnType.STRING, "United Kingdom", null),
                        new LikeDimFilter("page", "%b%", null, null).toFilter()
                    )
                ),
                new AndFilter(
                    ImmutableList.of(
                        new NullFilter("countryName", null),
                        new LikeDimFilter("page", "%c%", null, null).toFilter()
                    )
                )
            )
        ),
        cursorAutoArrangeFilters
    );

    Assert.assertEquals(
        "matcher: OR\n"
        + "  matcher: AND\n"
        + "    with partial index: AND (selectionSize = 11851)\n"
        + "      index: countryName IS NULL (selectionSize = 35445)\n"
        + "      index: page LIKE '%c%' (selectionSize = 12864)\n"
        + "  matcher: AND\n"
        + "    with partial index: countryName = United States (selectionSize = 528)\n"
        + "    matcher: page LIKE '%a%'\n"
        + "  matcher: AND\n"
        + "    with partial index: countryName = United Kingdom (selectionSize = 234)\n"
        + "    matcher: page LIKE '%b%'\n",
        filterBundle.getInfo().describe()
    );
  }

  @Test
  @Parameters({"true", "false"})
  public void test_or_countryIsNull_and_country_pageLike(boolean cursorAutoArrangeFilters)
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new OrFilter(
            ImmutableList.of(
                new NullFilter("countryName", null),
                new AndFilter(
                    ImmutableList.of(
                        new EqualityFilter("countryName", ColumnType.STRING, "United States", null),
                        new LikeDimFilter("page", "%a%", null, null).toFilter()
                    )
                )
            )
        ),
        cursorAutoArrangeFilters
    );

    Assert.assertEquals(
        "matcher: OR\n"
        + "  matcher: countryName IS NULL\n"
        + "    with partial index: countryName IS NULL (selectionSize = 35445)\n"
        + "  matcher: AND\n"
        + "    with partial index: countryName = United States (selectionSize = 528)\n"
        + "    matcher: page LIKE '%a%'\n",
        filterBundle.getInfo().describe()
    );
  }

  @Test
  @Parameters({"true", "false"})
  public void test_or_countryIsNull_and_isRobotInFalseTrue_pageLike(boolean cursorAutoArrangeFilters)
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new OrFilter(
            ImmutableList.of(
                new NullFilter("countryName", null),
                new AndFilter(
                    ImmutableList.of(
                        // isRobot IN (false, true) matches all rows; so this test is equivalent logically to
                        // test_or_countryIsNull_pageLike. It's effectively testing that the AndFilter carries through
                        // the short-circuiting done by the OrFilter when it applies the NullFilter.
                        new TypedInFilter("isRobot", ColumnType.STRING, ImmutableList.of("false", "true"), null, null),
                        new LikeDimFilter("page", "%u%", null, null).toFilter()
                    )
                )
            )
        ),
        cursorAutoArrangeFilters
    );

    Assert.assertEquals(
        "matcher: OR\n"
        + "  matcher: countryName IS NULL\n"
        + "    with partial index: countryName IS NULL (selectionSize = 35445)\n"
        + "  matcher: AND\n"
        + "    with partial index: isRobot IN (false, true) (STRING) (selectionSize = 39244)\n"
        + "    matcher: page LIKE '%u%'\n",
        filterBundle.getInfo().describe()
    );
  }

  protected FilterBundle makeFilterBundle(final Filter filter, boolean cursorAutoArrangeFilters)
  {
    return new FilterBundle.Builder(filter, indexSelector,
                                    cursorAutoArrangeFilters
    ).build(
        new DefaultBitmapResultFactory(bitmapFactory),
        indexSelector.getNumRows(),
        indexSelector.getNumRows(),
        false
    );
  }
}
