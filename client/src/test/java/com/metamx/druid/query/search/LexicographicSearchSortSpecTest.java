package com.metamx.druid.query.search;

import org.junit.Assert;
import org.junit.Test;

/**
 */
public class LexicographicSearchSortSpecTest
{
  @Test
  public void testComparator()
  {
    SearchHit hit1 = new SearchHit("test", "apple");
    SearchHit hit2 = new SearchHit("test", "banana");
    SearchHit hit3 = new SearchHit("test", "banana");

    SearchSortSpec spec = new LexicographicSearchSortSpec();

    Assert.assertTrue(spec.getComparator().compare(hit2, hit3) == 0);
    Assert.assertTrue(spec.getComparator().compare(hit2, hit1) > 0);
    Assert.assertTrue(spec.getComparator().compare(hit1, hit3) < 0);
  }
}
