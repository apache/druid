package com.metamx.druid.query.extraction;

import com.google.common.collect.Sets;
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
