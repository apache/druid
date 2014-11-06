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

package io.druid.testing.utils;

import java.util.Iterator;
import java.util.Map;

public class QueryResultVerifier
{
  public static boolean compareResults(
      Iterable<Map<String, Object>> actual,
      Iterable<Map<String, Object>> expected
  )
  {
    Iterator<Map<String, Object>> actualIter = actual.iterator();
    Iterator<Map<String, Object>> expectedIter = expected.iterator();

    while (actualIter.hasNext() && expectedIter.hasNext()) {
      Map<String, Object> actualRes = actualIter.next();
      Map<String, Object> expRes = expectedIter.next();

      if (!actualRes.equals(expRes)) {
        return false;
      }
    }

    if (actualIter.hasNext() || expectedIter.hasNext()) {
      return false;
    }
    return true;
  }
}
