package com.metamx.druid.query.extraction;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 */
public class RegexDimExtractionFnTest
{
  private static final String[] paths = {
      "/druid/prod/compute",
      "/druid/prod/bard",
      "/druid/prod/master",
      "/druid/demo-east/compute",
      "/druid/demo-east/bard",
      "/druid/demo-east/master",
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
    Assert.assertTrue(extracted.contains("druid/demo-east"));
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
