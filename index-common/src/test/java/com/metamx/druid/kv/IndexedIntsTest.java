package com.metamx.druid.kv;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 */
@RunWith(Parameterized.class)
public class IndexedIntsTest
{
  private static final int[] array = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

  private final IndexedInts indexed;

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    return Arrays.asList(
        new Object[][]{
            {VSizeIndexedInts.fromArray(array)},
            {IntBufferIndexedInts.fromArray(array)}
        }
    );
  }

  public IndexedIntsTest(
      IndexedInts indexed
  )
  {
    this.indexed = indexed;
  }

  @Test
  public void testSanity() throws Exception
  {
    Assert.assertEquals(array.length, indexed.size());
    for (int i = 0; i < array.length; i++) {
      Assert.assertEquals(array[i], indexed.get(i));
    }
  }
}
