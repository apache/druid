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

package io.druid.query.extraction.extraction;

import com.google.common.collect.Sets;
import io.druid.query.extraction.DimExtractionFn;
import io.druid.query.extraction.PartialDimExtractionFn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 */
public class PartialDimExtractionFnTest
{
  private static final String[] testStrings = {
      "Quito",
      "Calgary",
      "Tokyo",
      "Stockholm",
      "Vancouver",
      "Pretoria",
      "Wellington",
      null,
      "Ontario"
  };

  @Test
  public void testExtraction()
  {
    String regex = ".*[Tt][Oo].*";
    DimExtractionFn dimExtractionFn = new PartialDimExtractionFn(regex);
    List<String> expected = Arrays.asList("Quito", "Tokyo", "Stockholm", "Pretoria", "Wellington");
    Set<String> extracted = Sets.newHashSet();

    for (String str : testStrings) {
      String res = dimExtractionFn.apply(str);
      if (res != null) {
        extracted.add(res);
      }
    }

    Assert.assertEquals(5, extracted.size());

    for (String str : extracted) {
      Assert.assertTrue(expected.contains(str));
    }
  }
}
