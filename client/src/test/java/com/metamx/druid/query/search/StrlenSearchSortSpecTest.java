package com.metamx.druid.query.search;

import org.junit.Assert;
import org.junit.Test;

/**
 */
public class StrlenSearchSortSpecTest
{
  @Test
  public void testComparator()
  {
    SearchSortSpec spec = new StrlenSearchSortSpec();

    SearchHit hit1 = new SearchHit("test", "a");
    SearchHit hit2 = new SearchHit("test", "apple");
    SearchHit hit3 = new SearchHit("test", "elppa");

    Assert.assertTrue(spec.getComparator().compare(hit2, hit3) < 0);
    Assert.assertTrue(spec.getComparator().compare(hit2, hit1) > 0);
    Assert.assertTrue(spec.getComparator().compare(hit1, hit3) < 0);
  }
}
