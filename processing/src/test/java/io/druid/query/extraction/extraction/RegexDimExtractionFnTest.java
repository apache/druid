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
import io.druid.query.extraction.RegexDimExtractionFn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 */
public class RegexDimExtractionFnTest
{
  private static final String[] paths = {
      "/druid/prod/historical",
      "/druid/prod/broker",
      "/druid/prod/coordinator",
      "/druid/demo/historical",
      "/druid/demo/broker",
      "/druid/demo/coordinator",
      "/dash/aloe",
      "/dash/baloo"
  };

  private static final String[] testStrings = {
      "apple",
      "awesome",
      "asylum",
      "business",
      "be",
      "cool"
  };

  @Test
  public void testPathExtraction()
  {
    String regex = "/([^/]+)/";
    DimExtractionFn dimExtractionFn = new RegexDimExtractionFn(regex);
    Set<String> extracted = Sets.newHashSet();

    for (String path : paths) {
      extracted.add(dimExtractionFn.apply(path));
    }

    Assert.assertEquals(2, extracted.size());
    Assert.assertTrue(extracted.contains("druid"));
    Assert.assertTrue(extracted.contains("dash"));
  }

  @Test
  public void testDeeperPathExtraction()
  {
    String regex = "^/([^/]+/[^/]+)(/|$)";
    DimExtractionFn dimExtractionFn = new RegexDimExtractionFn(regex);
    Set<String> extracted = Sets.newHashSet();

    for (String path : paths) {
      extracted.add(dimExtractionFn.apply(path));
    }

    Assert.assertEquals(4, extracted.size());
    Assert.assertTrue(extracted.contains("druid/prod"));
    Assert.assertTrue(extracted.contains("druid/demo"));
    Assert.assertTrue(extracted.contains("dash/aloe"));
    Assert.assertTrue(extracted.contains("dash/baloo"));
  }

  @Test
  public void testStringExtraction()
  {
    String regex = "(.)";
    DimExtractionFn dimExtractionFn = new RegexDimExtractionFn(regex);
    Set<String> extracted = Sets.newHashSet();

    for (String testString : testStrings) {
      extracted.add(dimExtractionFn.apply(testString));
    }

    Assert.assertEquals(3, extracted.size());
    Assert.assertTrue(extracted.contains("a"));
    Assert.assertTrue(extracted.contains("b"));
    Assert.assertTrue(extracted.contains("c"));
  }
}
