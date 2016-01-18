/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.collections;

import org.junit.Assert;
import org.junit.Test;

import java.nio.IntBuffer;

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
  
  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testExceptionInGet()
  {
    IntList list = new IntList();
    list.get(1);
  }
  
  @Test
  public void testToArray()
  {
    int[] inputArray = {1,3,4};
    IntList list = new IntList();
    for (int i = 0; i < inputArray.length; i++) {
      list.add(inputArray[i]);
    }
    int[] outputArray = list.toArray();
    Assert.assertArrayEquals(inputArray, outputArray);
  }
  
  @Test
  public void testNullCaseGetBaseList()
  {
    final int intSize = 2;
    IntList list = new IntList(intSize);
    list.set(2 * intSize,100);
    IntBuffer outBuffer;
    outBuffer = list.getBaseList(0);
    Assert.assertNull("Should be Null",outBuffer);
  }
  
  @Test
  public void testGetBaseList()
  {
    int listSize = 2;
    IntList list = new IntList(listSize);
    int[] expectedArray = {1,2};
    list.add(expectedArray[0]);
    list.add(expectedArray[1]);
    IntBuffer outputBuffer = list.getBaseList(0);
    Assert.assertEquals("Buffer size does not match",2, outputBuffer.limit());
    int[] actualArray = new int[outputBuffer.capacity()];
    outputBuffer.get(actualArray);
    Assert.assertArrayEquals("Arrays are not matching",expectedArray, actualArray);
  }
}
