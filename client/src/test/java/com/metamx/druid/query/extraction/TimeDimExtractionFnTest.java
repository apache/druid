package com.metamx.druid.query.extraction;

import com.google.common.collect.Sets;
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
