/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.java.util.common;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MemoryBoundLinkedBlockingQueueTest
{
  @Test
  public void test_offer_emptyQueueWithEnoughCapacity_true()
  {

    long byteCapacity = 10L;
    MemoryBoundLinkedBlockingQueue<byte[]> queue = setupQueue(byteCapacity, ImmutableList.of());
    byte[] item = "item".getBytes(StandardCharsets.UTF_8);

    boolean succeeds = queue.offer(new MemoryBoundLinkedBlockingQueue.ObjectContainer<>(item, item.length));

    long expectedByteSize = item.length;
    Assert.assertTrue(succeeds);
    Assert.assertEquals(1, queue.size());
    Assert.assertEquals(expectedByteSize, queue.byteSize());
    Assert.assertEquals(byteCapacity - item.length, queue.remainingCapacity());
  }

  @Test
  public void test_offer_nonEmptyQueueWithEnoughCapacity_true()
  {

    long byteCapacity = 10L;
    byte[] item1 = "item1".getBytes(StandardCharsets.UTF_8);
    byte[] item2 = "item2".getBytes(StandardCharsets.UTF_8);
    Collection<MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]>> items = buildItemContainers(
        ImmutableList.of(item1)
    );
    MemoryBoundLinkedBlockingQueue<byte[]> queue = setupQueue(byteCapacity, items);

    boolean succeeds = queue.offer(new MemoryBoundLinkedBlockingQueue.ObjectContainer<>(item2, item2.length));

    long expectedByteSize = item1.length + item2.length;
    Assert.assertTrue(succeeds);
    Assert.assertEquals(2, queue.size());
    Assert.assertEquals(expectedByteSize, queue.byteSize());
    Assert.assertEquals(byteCapacity - expectedByteSize, queue.remainingCapacity());
  }

  @Test
  public void test_offer_queueWithoutEnoughCapacity_false()
  {

    long byteCapacity = 7L;
    byte[] item1 = "item1".getBytes(StandardCharsets.UTF_8);
    byte[] item2 = "item2".getBytes(StandardCharsets.UTF_8);
    Collection<MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]>> items = buildItemContainers(
        ImmutableList.of(item1)
    );
    MemoryBoundLinkedBlockingQueue<byte[]> queue = setupQueue(byteCapacity, items);

    boolean succeeds = queue.offer(new MemoryBoundLinkedBlockingQueue.ObjectContainer<>(item2, item2.length));

    long expectedByteSize = item1.length;
    Assert.assertFalse(succeeds);
    Assert.assertEquals(1, queue.size());
    Assert.assertEquals(expectedByteSize, queue.byteSize());
    Assert.assertEquals(byteCapacity - expectedByteSize, queue.remainingCapacity());
  }

  @Test
  public void test_offerWithTimeLimit_interruptedExceptinThrown_throws()
  {
    long byteCapacity = 10L;
    MemoryBoundLinkedBlockingQueue<byte[]> queue = setupQueue(
        byteCapacity,
        ImmutableList.of(),
        new InterruptedExceptionThrowingQueue()
    );
    byte[] item = "item".getBytes(StandardCharsets.UTF_8);

    Assert.assertThrows(
        InterruptedException.class,
        () -> queue.offer(
            new MemoryBoundLinkedBlockingQueue.ObjectContainer<>(item, item.length),
            1L,
            TimeUnit.MILLISECONDS
        )
    );

    Assert.assertEquals(0, queue.size());
    Assert.assertEquals(0L, queue.byteSize());
    Assert.assertEquals(byteCapacity, queue.remainingCapacity());
  }

  @Test
  public void test_offerWithTimeLimit_fullQueue_waitsTime() throws InterruptedException
  {
    long timeoutMillis = 2000L;
    long byteCapacity = 10L;
    byte[] item1 = "item1".getBytes(StandardCharsets.UTF_8);
    byte[] item2 = "item2".getBytes(StandardCharsets.UTF_8);
    Collection<MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]>> items = buildItemContainers(
        ImmutableList.of(item1, item2)
    );
    MemoryBoundLinkedBlockingQueue<byte[]> queue = setupQueue(
        byteCapacity,
        items,
        new InterruptedExceptionThrowingQueue()
    );
    byte[] item = "item".getBytes(StandardCharsets.UTF_8);
    long start = System.currentTimeMillis();
    boolean succeeds = queue.offer(
        new MemoryBoundLinkedBlockingQueue.ObjectContainer<>(item, item.length),
        timeoutMillis,
        TimeUnit.MILLISECONDS
    );
    long end = System.currentTimeMillis();

    Assert.assertFalse(succeeds);
    Assert.assertTrue(
        StringUtils.format(
            "offer only waited at most [%d] nanos instead of expected [%d] nanos",
            TimeUnit.MILLISECONDS.toNanos(end - start),
            TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
        ),
        TimeUnit.MILLISECONDS.toNanos(end - start) >= TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
    );
    Assert.assertEquals(2, queue.size());
    Assert.assertEquals(10L, queue.byteSize());
    Assert.assertEquals(0L, queue.remainingCapacity());
  }

  @Test
  public void test_take_nonEmptyQueue_expected() throws InterruptedException
  {

    long byteCapacity = 10L;
    byte[] item1 = "item1".getBytes(StandardCharsets.UTF_8);
    byte[] item2 = "item2".getBytes(StandardCharsets.UTF_8);
    MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]> item1Container =
        new MemoryBoundLinkedBlockingQueue.ObjectContainer<>(item1, item1.length);
    MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]> item2Container =
        new MemoryBoundLinkedBlockingQueue.ObjectContainer<>(item2, item2.length);
    MemoryBoundLinkedBlockingQueue<byte[]> queue = setupQueue(byteCapacity, ImmutableList.of());
    Assert.assertTrue(queue.offer(item1Container));
    Assert.assertTrue(queue.offer(item2Container));

    MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]> takenItem = queue.take();
    long expectedByteSize = item2.length;

    Assert.assertSame(item1Container, takenItem);
    Assert.assertEquals(1, queue.size());
    Assert.assertEquals(expectedByteSize, queue.byteSize());
    Assert.assertEquals(byteCapacity - expectedByteSize, queue.remainingCapacity());
  }

  @Test
  public void test_drain_emptyQueue_succeeds() throws InterruptedException
  {

    long byteCapacity = 7L;
    MemoryBoundLinkedBlockingQueue<byte[]> queue = setupQueue(byteCapacity, ImmutableList.of());
    Collection<MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]>> buffer = new ArrayList<>();

    int numAdded = queue.drain(buffer, 1, 1, TimeUnit.SECONDS);

    Assert.assertTrue(numAdded == 0 && numAdded == buffer.size());
    Assert.assertEquals(0, queue.size());
    Assert.assertEquals(0L, queue.byteSize());
    Assert.assertEquals(byteCapacity, queue.remainingCapacity());
  }

  @Test
  public void test_drain_queueWithOneItem_succeeds() throws InterruptedException
  {

    long byteCapacity = 7L;
    byte[] item1 = "item1".getBytes(StandardCharsets.UTF_8);
    Collection<MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]>> items = buildItemContainers(
        ImmutableList.of(item1)
    );
    MemoryBoundLinkedBlockingQueue<byte[]> queue = setupQueue(byteCapacity, items);
    Collection<MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]>> buffer = new ArrayList<>();

    int numAdded = queue.drain(buffer, 1, 1, TimeUnit.MINUTES);

    Assert.assertTrue(numAdded == 1 && numAdded == buffer.size());
    Assert.assertEquals(0, queue.size());
    Assert.assertEquals(0L, queue.byteSize());
    Assert.assertEquals(byteCapacity, queue.remainingCapacity());
  }

  @Test
  public void test_drain_queueWithMultipleItems_succeeds() throws InterruptedException
  {

    long byteCapacity = 15L;
    byte[] item1 = "item1".getBytes(StandardCharsets.UTF_8);
    byte[] item2 = "item2".getBytes(StandardCharsets.UTF_8);
    byte[] item3 = "item3".getBytes(StandardCharsets.UTF_8);
    Collection<MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]>> items = buildItemContainers(
        ImmutableList.of(item1, item2, item3)
    );
    MemoryBoundLinkedBlockingQueue<byte[]> queue = setupQueue(byteCapacity, items, new NotAllDrainedQueue());
    Collection<MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]>> buffer = new ArrayList<>();

    int numAdded = queue.drain(buffer, 10, 1, TimeUnit.MINUTES);

    Assert.assertTrue(numAdded == 2 && numAdded == buffer.size());
    Assert.assertEquals(1, queue.size());
    Assert.assertEquals(item3.length, queue.byteSize());
    Assert.assertEquals(byteCapacity - item3.length, queue.remainingCapacity());
  }

  @Test
  public void test_drain_queueWithFirstItemSizeGreaterThanLimit_succeeds() throws InterruptedException
  {

    long byteCapacity = 15L;
    byte[] item1 = "item1".getBytes(StandardCharsets.UTF_8);
    byte[] item2 = "item2".getBytes(StandardCharsets.UTF_8);
    byte[] item3 = "item3".getBytes(StandardCharsets.UTF_8);
    Collection<MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]>> items = buildItemContainers(
        ImmutableList.of(item1, item2, item3)
    );
    MemoryBoundLinkedBlockingQueue<byte[]> queue = setupQueue(byteCapacity, items, new NotAllDrainedQueue());
    Collection<MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]>> buffer = new ArrayList<>();

    int numAdded = queue.drain(buffer, item1.length - 1, 1, TimeUnit.MINUTES);

    Assert.assertTrue(numAdded == 1 && numAdded == buffer.size());
    Assert.assertEquals(2, queue.size());
    Assert.assertEquals(item2.length + item3.length, queue.byteSize());
    Assert.assertEquals(byteCapacity - (item2.length + item3.length), queue.remainingCapacity());
  }

  private static <T> MemoryBoundLinkedBlockingQueue<T> setupQueue(
      long byteCapacity,
      Collection<MemoryBoundLinkedBlockingQueue.ObjectContainer<T>> items
  )
  {
    return setupQueue(byteCapacity, items, null);
  }

  private static <T> MemoryBoundLinkedBlockingQueue<T> setupQueue(
      long byteCapacity,
      Collection<MemoryBoundLinkedBlockingQueue.ObjectContainer<T>> items,
      @Nullable LinkedBlockingQueue<MemoryBoundLinkedBlockingQueue.ObjectContainer<T>> underlyingQueue
  )
  {
    Assert.assertTrue(getTotalSizeOfItems(items) <= byteCapacity);
    MemoryBoundLinkedBlockingQueue<T> queue = underlyingQueue != null ?
        new MemoryBoundLinkedBlockingQueue<>(underlyingQueue, byteCapacity) : new MemoryBoundLinkedBlockingQueue<>(byteCapacity);
    items.forEach(i -> Assert.assertTrue(queue.offer(i)));
    return queue;
  }

  private static Collection<MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]>> buildItemContainers(
      Collection<byte[]> items
  )
  {
    return items.stream()
        .map(i -> new MemoryBoundLinkedBlockingQueue.ObjectContainer<>(i, i.length))
        .collect(Collectors.toList());
  }

  private static <T> long getTotalSizeOfItems(Collection<MemoryBoundLinkedBlockingQueue.ObjectContainer<T>> items)
  {
    return items.stream().mapToLong(MemoryBoundLinkedBlockingQueue.ObjectContainer::getSize).sum();
  }

  static class InterruptedExceptionThrowingQueue
      extends LinkedBlockingQueue<MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]>>
  {
    @Override
    public boolean offer(MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]> item, long timeout, TimeUnit unit)
        throws InterruptedException
    {
      throw new InterruptedException("exception thrown");
    }
  }

  static class NotAllDrainedQueue
      extends LinkedBlockingQueue<MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]>>
  {
    @Override
    public int drainTo(Collection<? super MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]>> c, int maxElements)
    {
      MemoryBoundLinkedBlockingQueue.ObjectContainer<byte[]> firstItem = this.poll();
      c.add(firstItem);
      return 1;
    }
  }
}
