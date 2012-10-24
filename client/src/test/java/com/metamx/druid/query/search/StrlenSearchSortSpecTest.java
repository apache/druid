/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.query.search;

import org.junit.Assert;
import org.junit.Test;

/**
 */
public class StrlenSearchSortSpecTest
{
  @Test
  public void testComparator()
  {
    SearchSortSpec spec = new StrlenSearchSortSpec();

    SearchHit hit1 = new SearchHit("test", "a");
    SearchHit hit2 = new SearchHit("test", "apple");
    SearchHit hit3 = new SearchHit("test", "elppa");

    Assert.assertTrue(spec.getComparator().compare(hit2, hit3) < 0);
    Assert.assertTrue(spec.getComparator().compare(hit2, hit1) > 0);
    Assert.assertTrue(spec.getComparator().compare(hit1, hit3) < 0);
  }
}
