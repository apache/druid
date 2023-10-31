package org.apache.druid.indexing.kinesis;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MemoryBoundLinkedBlockingQueue<E> extends LinkedBlockingQueue<E> {
  private final LinkedBlockingQueue<E> queue;
  private final long byteLimit;
  private long currentSizeBytes;

  public MemoryBoundLinkedBlockingQueue(LinkedBlockingQueue<E> queue, long byteLimit) {
    this.queue = queue;
    this.byteLimit = byteLimit;
    this.currentSizeBytes = 0L;
  }

  /**
   * Inserts the specified element into this queue, waiting up to the
   * specified wait time if necessary for space to become available.
   *
   * @param e       the element to add
   * @param timeout how long to wait before giving up, in units of
   *                {@code unit}
   * @param unit    a {@code TimeUnit} determining how to interpret the
   *                {@code timeout} parameter
   *
   * @return {@code true} if successful, or {@code false} if
   * the specified waiting time elapses before space is available
   * @throws InterruptedException     if interrupted while waiting
   * @throws ClassCastException       if the class of the specified element
   *                                  prevents it from being added to this queue
   * @throws NullPointerException     if the specified element is null
   * @throws IllegalArgumentException if some property of the specified
   *                                  element prevents it from being added to this queue
   */
  public boolean offer(E e, long sizeBytes, long timeout, TimeUnit unit)
      throws InterruptedException {
    synchronized (this) {
      if (currentSizeBytes >= byteLimit) {
        return false;
      }
      boolean successful = queue.offer(e, timeout, unit);
      if (successful) {
        currentSizeBytes += sizeBytes;
      }
      return successful;
    }
  }

  @Override
  public int remainingCapacity() {
    return (int) (byteLimit - currentSizeBytes);
  }

  public long currentByteSize() {
    return currentSizeBytes;
  }
}
