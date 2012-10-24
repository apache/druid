package com.metamx.druid.kv;

import org.junit.Assert;
import org.junit.Test;

/**
 */
public class VSizeIndexedIntsTest
{
  @Test
  public void testSanity() throws Exception
  {
    final int[] array = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    VSizeIndexedInts ints = VSizeIndexedInts.fromArray(array);

    Assert.assertEquals(1, ints.getNumBytes());
    Assert.assertEquals(array.length, ints.size());
    for (int i = 0; i < array.length; i++) {
      Assert.assertEquals(array[i], ints.get(i));
    }
  }
}
