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

package io.druid.query.groupby.epinephelinae;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class ByteBufferMinMaxOffsetHeapTest
{
  @Test
  public void testSimple()
  {
    int limit = 15;
    ByteBuffer myBuffer = ByteBuffer.allocate(1000000);
    ByteBufferMinMaxOffsetHeap heap = new ByteBufferMinMaxOffsetHeap(myBuffer, limit, Ordering.<Integer>natural(), null);

    ArrayList<Integer> values = Lists.newArrayList(
        30, 45, 81, 92, 68, 54, 66, 33, 89, 98,
        87, 62, 84, 39, 13, 32, 67, 50, 21, 53,
        93, 18, 86, 41, 14, 56, 51, 69, 91, 60,
        6, 2, 79, 4, 35, 17, 71, 22, 29, 76,
        57, 97, 73, 24, 94, 77, 80, 15, 52, 88,
        95, 96, 9, 3, 48, 58, 75, 82, 90, 65,
        36, 85, 20, 34, 37, 72, 11, 78, 28, 43,
        27, 12, 83, 38, 59, 19, 31, 46, 40, 63,
        23, 70, 26, 8, 64, 16, 10, 74, 7, 25,
        5, 42, 47, 44, 1, 49, 99
    );

    for (int i = 0; i < values.size(); i++){
      heap.addOffset(values.get(i));
    }

    int x = heap.removeAt(8);
    heap.addOffset(x);

    x = heap.removeAt(2);
    heap.addOffset(x);

    Collections.sort(values);
    List<Integer> expected = values.subList(0, limit);

    List<Integer> actual = Lists.newArrayList();
    for (int i = 0; i < limit; i++) {
      int min = heap.removeMin();
      actual.add(min);
    }

    Assert.assertEquals(expected, actual);
  }


  @Test
  public void testRandom()
  {
    int limit = 20;

    Random rng = new Random(999);

    ArrayList<Integer> values = Lists.newArrayList();
    for (int i = 0; i < 10000; i++) {
      values.add(rng.nextInt(1000000));
    }
    ArrayList<Integer> deletedValues = Lists.newArrayList();

    ByteBuffer myBuffer = ByteBuffer.allocate(1000000);
    ByteBufferMinMaxOffsetHeap heap = new ByteBufferMinMaxOffsetHeap(myBuffer, limit, Ordering.<Integer>natural(), null);

    for (int i = 0; i < values.size(); i++){
      int droppedOffset = heap.addOffset(values.get(i));
      Assert.assertTrue(heap.isIntact());

      if (droppedOffset > 0) {
        deletedValues.add(droppedOffset);
      }

      // 15% chance to delete a random value for every two values added when heap is > 50% full
      if (heap.getHeapSize() > (limit / 2) && i % 2 == 1) {
        double deleteRoll = rng.nextDouble();
        if (deleteRoll > 0.15) {
          int indexToRemove = rng.nextInt(heap.getHeapSize());
          int deadOffset = heap.removeAt(indexToRemove);
          Assert.assertTrue(heap.isIntact());
          deletedValues.add(deadOffset);
        }
      }
    }

    Collections.sort(values);
    Collections.sort(deletedValues);

    for (int deletedValue : deletedValues) {
      int idx = values.indexOf(deletedValue);
      values.remove(idx);
    }

    Assert.assertTrue(heap.getHeapSize() <= limit);
    List<Integer> expected = values.subList(0, heap.getHeapSize());

    List<Integer> actual = Lists.newArrayList();
    int initialHeapSize = heap.getHeapSize();
    for (int i = 0; i < initialHeapSize; i++){
      int min = heap.removeMin();
      actual.add(min);
    }

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testRandom2()
  {
    int limit = 5000;

    Random rng = new Random(9999);

    ArrayList<Integer> values = Lists.newArrayList();
    for (int i = 0; i < 20000; i++) {
      values.add(rng.nextInt(1000000));
    }
    ArrayList<Integer> deletedValues = Lists.newArrayList();

    ByteBuffer myBuffer = ByteBuffer.allocate(1000000);
    ByteBufferMinMaxOffsetHeap heap = new ByteBufferMinMaxOffsetHeap(myBuffer, limit, Ordering.<Integer>natural(), null);

    for (int i = 0; i < values.size(); i++){
      int droppedOffset = heap.addOffset(values.get(i));
      Assert.assertTrue(heap.isIntact());

      if (droppedOffset > 0) {
        deletedValues.add(droppedOffset);
      }

      // 15% chance to delete a random value for every two values added when heap is > 50% full
      if (heap.getHeapSize() > (limit / 2) && i % 2 == 1) {
        double deleteRoll = rng.nextDouble();
        if (deleteRoll > 0.15) {
          int indexToRemove = rng.nextInt(heap.getHeapSize());
          int deadOffset = heap.removeAt(indexToRemove);
          Assert.assertTrue(heap.isIntact());
          deletedValues.add(deadOffset);
        }
      }
    }

    Collections.sort(values);
    Collections.sort(deletedValues);

    for (int deletedValue : deletedValues) {
      int idx = values.indexOf(deletedValue);
      values.remove(idx);
    }

    Assert.assertTrue(heap.getHeapSize() <= limit);
    List<Integer> expected = values.subList(0, heap.getHeapSize());

    List<Integer> actual = Lists.newArrayList();
    int initialHeapSize = heap.getHeapSize();
    for (int i = 0; i < initialHeapSize; i++){
      int min = heap.removeMin();
      actual.add(min);
    }

    Assert.assertEquals(expected, actual);
  }


  @Test
  public void testRemove()
  {
    int limit = 100;

    IntList values = new IntArrayList(new int[] {
        1, 20, 1000, 2, 3, 30, 40, 10, 11, 12, 13, 300, 400, 500, 600
    });

    ByteBuffer myBuffer = ByteBuffer.allocate(1000000);
    ByteBufferMinMaxOffsetHeap heap = new ByteBufferMinMaxOffsetHeap(myBuffer, limit, Ordering.<Integer>natural(), null);

    for (int i = 0; i < values.size(); i++){
      heap.addOffset(values.get(i));
      Assert.assertTrue(heap.isIntact());
    }

    heap.removeOffset(12);

    Assert.assertTrue(heap.isIntact());

    Collections.sort(values);
    values.rem(12);

    List<Integer> actual = Lists.newArrayList();
    for (int i = 0; i < values.size(); i++){
      int min = heap.removeMin();
      actual.add(min);
    }

    Assert.assertEquals(values, actual);
  }

  @Test
  public void testRemove2()
  {
    int limit = 100;

    IntList values = new IntArrayList(new int[] {
        1, 20, 1000, 2, 3, 30, 40, 10, 11, 12, 13, 300, 400, 500, 600, 4, 5,
        6, 7, 8, 9, 4, 5, 200, 250
    });

    ByteBuffer myBuffer = ByteBuffer.allocate(1000000);
    ByteBufferMinMaxOffsetHeap heap = new ByteBufferMinMaxOffsetHeap(myBuffer, limit, Ordering.<Integer>natural(), null);

    for (int i = 0; i < values.size(); i++){
      heap.addOffset(values.get(i));
    }
    Assert.assertTrue(heap.isIntact());

    heap.removeOffset(2);
    Assert.assertTrue(heap.isIntact());

    Collections.sort(values);
    values.rem(2);
    Assert.assertTrue(heap.isIntact());

    List<Integer> actual = Lists.newArrayList();
    for (int i = 0; i < values.size(); i++){
      int min = heap.removeMin();
      actual.add(min);
    }

    Assert.assertTrue(heap.isIntact());

    Assert.assertEquals(values, actual);
  }
}
