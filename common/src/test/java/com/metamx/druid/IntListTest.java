package com.metamx.druid;

import org.junit.Assert;
import org.junit.Test;

/**
 */
public class IntListTest
{
  @Test
  public void testAdd() throws Exception
  {
    IntList list = new IntList(5);

    Assert.assertEquals(0, list.length());

    for (int i = 0; i < 25; ++i) {
      list.add(i);
      Assert.assertEquals(i + 1, list.length());
    }

    Assert.assertEquals(25, list.length());

    for (int i = 0; i < list.length(); ++i) {
      Assert.assertEquals(i, list.get(i));
    }
  }

  @Test
  public void testSet() throws Exception
  {
    IntList list = new IntList(5);

    Assert.assertEquals(0, list.length());

    list.set(127, 29302);

    Assert.assertEquals(128, list.length());

    for (int i = 0; i < 127; ++i) {
      Assert.assertEquals(0, list.get(i));
    }

    Assert.assertEquals(29302, list.get(127));

    list.set(7, 2);
    Assert.assertEquals(128, list.length());
    Assert.assertEquals(2, list.get(7));

    list.set(7, 23);
    Assert.assertEquals(128, list.length());
    Assert.assertEquals(23, list.get(7));
  }
}
