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
import io.druid.query.extraction.TimeDimExtractionFn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 */
public class TimeDimExtractionFnTest
{
  private static final String[] dims = {
      "01/01/2012",
      "01/02/2012",
      "03/03/2012",
      "03/04/2012",
      "05/05/2012",
      "12/21/2012"
  };

  @Test
  public void testMonthExtraction()
  {
    Set<String> months = Sets.newHashSet();
    DimExtractionFn dimExtractionFn = new TimeDimExtractionFn("MM/dd/yyyy", "MM/yyyy");

    for (String dim : dims) {
      months.add(dimExtractionFn.apply(dim));
    }

    Assert.assertEquals(months.size(), 4);
    Assert.assertTrue(months.contains("01/2012"));
    Assert.assertTrue(months.contains("03/2012"));
    Assert.assertTrue(months.contains("05/2012"));
    Assert.assertTrue(months.contains("12/2012"));
  }

  @Test
  public void testQuarterExtraction()
  {
    Set<String> quarters = Sets.newHashSet();
    DimExtractionFn dimExtractionFn = new TimeDimExtractionFn("MM/dd/yyyy", "QQQ/yyyy");

    for (String dim : dims) {
      quarters.add(dimExtractionFn.apply(dim));
    }

    Assert.assertEquals(quarters.size(), 3);
    Assert.assertTrue(quarters.contains("Q1/2012"));
    Assert.assertTrue(quarters.contains("Q2/2012"));
    Assert.assertTrue(quarters.contains("Q4/2012"));
  }
}
