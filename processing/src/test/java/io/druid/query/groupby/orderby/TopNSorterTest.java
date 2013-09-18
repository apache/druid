/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.groupby.orderby;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;


@RunWith(Parameterized.class)
public class TopNSorterTest
{
  private static final long SEED = 2L;
  private static final Ordering<String> ASC = Ordering.natural();
  private static final Ordering<String> DESC = Ordering.natural().reverse();

  private static final List<String> EMPTY = Collections.EMPTY_LIST;
  private static final List<String> SINGLE = Lists.newArrayList("a");
  private static final List<String> RAW_ASC = Lists.newArrayList(Splitter.fixedLength(1).split("abcdefghijk"));
  private static final List<String> RAW_DESC = Lists.newArrayList(Splitter.fixedLength(1).split("kjihgfedcba"));

  private Ordering<String> ordering;
  private List<String> rawInput;
  private int limit;

  @Parameterized.Parameters
  public static Collection<Object[]> makeTestData(){
    Object[][] data = new Object[][] {
      { ASC, RAW_ASC, RAW_ASC.size() - 2},
      { ASC, RAW_ASC, RAW_ASC.size()},
      { ASC, RAW_ASC, RAW_ASC.size() + 2},
      { ASC, RAW_ASC, 0},
      { ASC, SINGLE, 0},
      { ASC, SINGLE, 1},
      { ASC, SINGLE, 2},
      { ASC, SINGLE, 3},
      { ASC, EMPTY, 0},
      { ASC, EMPTY, 1},
      { DESC, RAW_DESC, RAW_DESC.size() - 2},
      { DESC, RAW_DESC, RAW_DESC.size()},
      { DESC, RAW_DESC, RAW_DESC.size() + 2},
      { DESC, RAW_DESC, 0},
      { DESC, RAW_DESC, 0},
      { DESC, SINGLE, 1},
      { DESC, SINGLE, 2},
      { DESC, SINGLE, 3},
      { DESC, EMPTY, 0},
      { DESC, EMPTY, 1},
    };

    return Arrays.asList(data);
  }

  public TopNSorterTest(Ordering<String> ordering, List<String> rawInput, int limit){
    this.ordering = ordering;
    this.rawInput = rawInput;
    this.limit = limit;
  }

  @Test
  public void testOrderByWithLimit()
  {
    List<String> expected = rawInput.subList(0, Math.min(limit, rawInput.size()));
    List<String> inputs = Lists.newArrayList(rawInput);
    Collections.shuffle(inputs, new Random(2));

    Iterable<String> result = new TopNSorter<String>(ordering).toTopN(inputs, limit);

    Assert.assertEquals(expected, Lists.newArrayList(result));
  }
}
