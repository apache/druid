package com.metamx.druid.query.extraction;

import com.google.common.collect.Sets;
import com.metamx.druid.query.search.FragmentSearchQuerySpec;
import com.metamx.druid.query.search.LexicographicSearchSortSpec;
import com.metamx.druid.query.search.SearchQuerySpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 */
public class SearchQuerySpecDimExtractionFnTest
{
  private static final String[] testStrings = {
      "Kyoto",
      "Calgary",
      "Tokyo",
      "Stockholm",
      "Toyokawa",
      "Pretoria",
      "Yorktown",
      "Ontario"
  };

  @Test
  public void testExtraction()
  {
    SearchQuerySpec spec = new FragmentSearchQuerySpec(
        Arrays.asList("to", "yo"), new LexicographicSearchSortSpec()
    );
    DimExtractionFn dimExtractionFn = new SearchQuerySpecDimExtractionFn(spec);
    List<String> expected = Arrays.asList("Kyoto", "Tokyo", "Toyokawa", "Yorktown");
    Set<String> extracted = Sets.newHashSet();

    for (String str : testStrings) {
      String res = dimExtractionFn.apply(str);
      if (res != null) {
        extracted.add(res);
      }
    }

    Assert.assertEquals(4, extracted.size());

    for (String str : extracted) {
      Assert.assertTrue(expected.contains(str));
    }
  }
}
