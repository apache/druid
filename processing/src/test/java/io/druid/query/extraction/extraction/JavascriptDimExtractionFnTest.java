package io.druid.query.extraction.extraction;

import io.druid.query.extraction.DimExtractionFn;
import io.druid.query.extraction.JavascriptDimExtractionFn;
import org.junit.Assert;
import org.junit.Test;

public class JavascriptDimExtractionFnTest
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
    String function = "function(str) { return str.substring(0,3); }";
    DimExtractionFn dimExtractionFn = new JavascriptDimExtractionFn(function);

    for (String str : testStrings) {
      String res = dimExtractionFn.apply(str);
      Assert.assertEquals(str.substring(0, 3), res);
    }
  }
}
