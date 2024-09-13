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
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class FilterBundleTest extends InitializedNullHandlingTest
{
  private Closer closer;
  protected BitmapFactory bitmapFactory;
  protected ColumnIndexSelector indexSelector;

  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder();

  @Parameters
  public static Object[] flags()
  {
    return new Object[]{false, true};
  }

  @Parameter
  public boolean cursorAutoArrangeFilters;

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
  public void test_or_country_isRobot()
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new OrFilter(
            ImmutableList.of(
                new EqualityFilter("countryName", ColumnType.STRING, "United States", null),
                new EqualityFilter("isRobot", ColumnType.STRING, "true", null)
            )
        )
    );

    Assert.assertEquals(
        "index: OR (selectionSize = 39244)\n"
        + "  index: countryName = United States (selectionSize = 528)\n"
        + "  index: isRobot = true (selectionSize = 15420)\n",
        filterBundle.getInfo().describe()
    );
  }

  @Test
  public void test_and_country_isRobot()
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new AndFilter(
            ImmutableList.of(
                new EqualityFilter("countryName", ColumnType.STRING, "United States", null),
                new EqualityFilter("isRobot", ColumnType.STRING, "true", null)
            )
        )
    );

    Assert.assertEquals(
        "index: AND (selectionSize = 0)\n"
        + "  index: countryName = United States (selectionSize = 528)\n"
        + "  index: isRobot = true (selectionSize = 15420)\n",
        filterBundle.getInfo().describe()
    );
  }

  @Test
  public void test_or_countryIsNull_pageLike()
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new OrFilter(
            ImmutableList.of(
                new NullFilter("countryName", null),
                new LikeDimFilter("page", "%u%", null, null).toFilter()
            )
        )
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
  public void test_and_countryIsNull_pageLike()
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new AndFilter(
            ImmutableList.of(
                new NullFilter("countryName", null),
                new LikeDimFilter("page", "%u%", null, null).toFilter()
            )
        )
    );

    Assert.assertEquals(
        "index: AND (selectionSize = 14165)\n"
        + "  index: countryName IS NULL (selectionSize = 35445)\n"
        + "  index: page LIKE '%u%' (selectionSize = 15328)\n",
        filterBundle.getInfo().describe()
    );
  }

  @Test
  public void test_and_country_pageLike()
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new AndFilter(
            ImmutableList.of(
                new EqualityFilter("countryName", ColumnType.STRING, "United States", null),
                new LikeDimFilter("page", "%u%", null, null).toFilter()
            )
        )
    );

    Assert.assertEquals(
        "index: countryName = United States (selectionSize = 528)\n"
        + "matcher: page LIKE '%u%'\n",
        filterBundle.getInfo().describe()
    );
  }

  @Test
  public void test_or_countryNotNull_pageLike()
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new OrFilter(
            ImmutableList.of(
                new NotFilter(new NullFilter("countryName", null)),
                new LikeDimFilter("page", "%u%", null, null).toFilter()
            )
        )
    );

    Assert.assertEquals(
        "index: OR (selectionSize = 39244)\n"
        + "  index: ~(countryName IS NULL) (selectionSize = 3799)\n"
        + "  index: page LIKE '%u%' (selectionSize = 15328)\n",
        filterBundle.getInfo().describe()
    );
  }

  @Test
  public void test_and_countryNotNull_pageLike()
  {
    final FilterBundle filterBundle = makeFilterBundle(
        new AndFilter(
            ImmutableList.of(
                new NotFilter(new NullFilter("countryName", null)),
                new LikeDimFilter("page", "%u%", null, null).toFilter()
            )
        )
    );

    Assert.assertEquals(
        "index: ~(countryName IS NULL) (selectionSize = 3799)\n"
        + "matcher: page LIKE '%u%'\n",
        filterBundle.getInfo().describe()
    );
  }

  @Test
  public void test_or_countryIsAndPageLike()
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
        )
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
  public void test_or_countryIsNull_and_country_pageLike()
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
        )
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
  public void test_or_countryIsNull_and_isRobotInFalseTrue_pageLike()
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
        )
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

  protected FilterBundle makeFilterBundle(final Filter filter)
  {
    return new FilterBundle.Builder(filter, indexSelector, cursorAutoArrangeFilters).build(
        new DefaultBitmapResultFactory(bitmapFactory),
        indexSelector.getNumRows(),
        indexSelector.getNumRows(),
        false
    );
  }
}
