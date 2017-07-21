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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.ISE;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * ByteBuffer-based implementation of the min-max heap developed by Atkinson, et al.
 * (http://portal.acm.org/citation.cfm?id=6621), with some utility functions from
 * Guava's MinMaxPriorityQueue.
 */
public class ByteBufferMinMaxOffsetHeap
{
  private static final int EVEN_POWERS_OF_TWO = 0x55555555;
  private static final int ODD_POWERS_OF_TWO = 0xaaaaaaaa;

  private final Comparator minComparator;
  private final Comparator maxComparator;
  private final ByteBuffer buf;
  private final int limit;
  private final LimitedBufferGrouper.BufferGrouperOffsetHeapIndexUpdater heapIndexUpdater;

  private int heapSize;

  public ByteBufferMinMaxOffsetHeap(
      ByteBuffer buf,
      int limit,
      Comparator<Integer> minComparator,
      LimitedBufferGrouper.BufferGrouperOffsetHeapIndexUpdater heapIndexUpdater
  )
  {
    this.buf = buf;
    this.limit = limit;
    this.heapSize = 0;
    this.minComparator = minComparator;
    this.maxComparator = Ordering.from(minComparator).reverse();
    this.heapIndexUpdater = heapIndexUpdater;
  }

  public void reset()
  {
    heapSize = 0;
  }

  public int addOffset(int offset)
  {
    int pos = heapSize;
    buf.putInt(pos * Ints.BYTES, offset);
    heapSize++;

    if (heapIndexUpdater != null) {
      heapIndexUpdater.updateHeapIndexForOffset(offset, pos);
    }

    bubbleUp(pos);

    if (heapSize > limit) {
      return removeMax();
    } else {
      return -1;
    }
  }

  public int removeMin()
  {
    if (heapSize < 1) {
      throw new ISE("Empty heap");
    }
    int minOffset = buf.getInt(0);
    if (heapIndexUpdater != null) {
      heapIndexUpdater.updateHeapIndexForOffset(minOffset, -1);
    }

    if (heapSize == 1) {
      heapSize--;
      return minOffset;
    }

    int lastIndex = heapSize - 1;
    int lastOffset = buf.getInt(lastIndex * Ints.BYTES);
    heapSize--;
    buf.putInt(0, lastOffset);

    if (heapIndexUpdater != null) {
      heapIndexUpdater.updateHeapIndexForOffset(lastOffset, 0);
    }

    Comparator comparator = isEvenLevel(0) ? minComparator : maxComparator;
    siftDown(comparator, 0);

    return minOffset;
  }

  public int removeMax()
  {
    int maxOffset;
    if (heapSize < 1) {
      throw new ISE("Empty heap");
    }
    if (heapSize == 1) {
      heapSize--;
      maxOffset = buf.getInt(0);
      if (heapIndexUpdater != null) {
        heapIndexUpdater.updateHeapIndexForOffset(maxOffset, -1);
      }
      return maxOffset;
    }

    // index of max must be 1, just remove it and shrink the heap
    if (heapSize == 2) {
      heapSize--;
      maxOffset = buf.getInt(Ints.BYTES);
      if (heapIndexUpdater != null) {
        heapIndexUpdater.updateHeapIndexForOffset(maxOffset, -1);
      }
      return maxOffset;
    }

    int maxIndex = findMaxElementIndex();
    maxOffset = buf.getInt(maxIndex * Ints.BYTES);

    int lastIndex = heapSize - 1;
    int lastOffset = buf.getInt(lastIndex * Ints.BYTES);
    heapSize--;
    buf.putInt(maxIndex * Ints.BYTES, lastOffset);

    if (heapIndexUpdater != null) {
      heapIndexUpdater.updateHeapIndexForOffset(maxOffset, -1);
      heapIndexUpdater.updateHeapIndexForOffset(lastOffset, maxIndex);
    }

    Comparator comparator = isEvenLevel(maxIndex) ? minComparator : maxComparator;
    siftDown(comparator, maxIndex);

    return maxOffset;
  }

  public int removeAt(int deletedIndex)
  {
    if (heapSize < 1) {
      throw new ISE("Empty heap");
    }
    int deletedOffset = buf.getInt(deletedIndex * Ints.BYTES);
    if (heapIndexUpdater != null) {
      heapIndexUpdater.updateHeapIndexForOffset(deletedOffset, -1);
    }

    int lastIndex = heapSize - 1;
    heapSize--;
    if (lastIndex == deletedIndex) {
      return deletedOffset;
    }
    int lastOffset = buf.getInt(lastIndex * Ints.BYTES);
    buf.putInt(deletedIndex * Ints.BYTES, lastOffset);

    if (heapIndexUpdater != null) {
      heapIndexUpdater.updateHeapIndexForOffset(lastOffset, deletedIndex);
    }

    Comparator comparator = isEvenLevel(deletedIndex) ? minComparator : maxComparator;

    bubbleUp(deletedIndex);
    siftDown(comparator, deletedIndex);

    return deletedOffset;
  }

  public void setAt(int index, int newVal)
  {
    buf.putInt(index * Ints.BYTES, newVal);
  }

  public int getAt(int index)
  {
    return buf.getInt(index * Ints.BYTES);
  }

  public int indexOf(int offset)
  {
    for (int i = 0; i < heapSize; i++) {
      int curOffset = buf.getInt(i * Ints.BYTES);
      if (curOffset == offset) {
        return i;
      }
    }
    return -1;
  }

  public void removeOffset(int offset)
  {
    int index = indexOf(offset);
    if (index > -1) {
      removeAt(index);
    }
  }

  public int getHeapSize()
  {
    return heapSize;
  }

  private void bubbleUp(int pos)
  {
    if (isEvenLevel(pos)) {
      int parentIndex = getParentIndex(pos);
      if (parentIndex > -1) {
        int parentOffset = buf.getInt(parentIndex * Ints.BYTES);
        int offset = buf.getInt(pos * Ints.BYTES);
        if (minComparator.compare(offset, parentOffset) > 0) {
          buf.putInt(parentIndex * Ints.BYTES, offset);
          buf.putInt(pos * Ints.BYTES, parentOffset);
          if (heapIndexUpdater != null) {
            heapIndexUpdater.updateHeapIndexForOffset(offset, parentIndex);
            heapIndexUpdater.updateHeapIndexForOffset(parentOffset, pos);
          }
          bubbleUpDirectional(maxComparator, parentIndex);
        } else {
          bubbleUpDirectional(minComparator, pos);
        }
      } else {
        bubbleUpDirectional(minComparator, pos);
      }
    } else {
      int parentIndex = getParentIndex(pos);
      if (parentIndex > -1) {
        int parentOffset = buf.getInt(parentIndex * Ints.BYTES);
        int offset = buf.getInt(pos * Ints.BYTES);
        if (minComparator.compare(offset, parentOffset) < 0) {
          buf.putInt(parentIndex * Ints.BYTES, offset);
          buf.putInt(pos * Ints.BYTES, parentOffset);
          if (heapIndexUpdater != null) {
            heapIndexUpdater.updateHeapIndexForOffset(offset, parentIndex);
            heapIndexUpdater.updateHeapIndexForOffset(parentOffset, pos);
          }
          bubbleUpDirectional(minComparator, parentIndex);
        } else {
          bubbleUpDirectional(maxComparator, pos);
        }
      } else {
        bubbleUpDirectional(maxComparator, pos);
      }
    }
  }

  private void bubbleUpDirectional(Comparator comparator, int pos)
  {
    int grandparent = getGrandparentIndex(pos);
    while (grandparent > -1) {
      int offset = buf.getInt(pos * Ints.BYTES);
      int gpOffset = buf.getInt(grandparent * Ints.BYTES);

      if (comparator.compare(offset, gpOffset) < 0) {
        buf.putInt(pos * Ints.BYTES, gpOffset);
        buf.putInt(grandparent * Ints.BYTES, offset);
        if (heapIndexUpdater != null) {
          heapIndexUpdater.updateHeapIndexForOffset(gpOffset, pos);
          heapIndexUpdater.updateHeapIndexForOffset(offset, grandparent);
        }
      }
      pos = grandparent;
      grandparent = getGrandparentIndex(pos);
    }
  }

  private void siftDown(Comparator comparator, int pos)
  {
    int minChild = findMinChild(comparator, pos);
    int minGrandchild;
    int minIndex;
    while (minChild > -1) {
      minGrandchild = findMinGrandChild(comparator, pos);
      if (minGrandchild > -1) {
        int minChildOffset = buf.getInt(minChild * Ints.BYTES);
        int minGcOffset = buf.getInt(minGrandchild * Ints.BYTES);
        int cmp = comparator.compare(minChildOffset, minGcOffset);
        minIndex = (cmp > 0) ? minGrandchild : minChild;
      } else if (minChild > -1) {
        minIndex = minChild;
      } else {
        break;
      }
      if (minIndex == minGrandchild) {
        int offset = buf.getInt(pos * Ints.BYTES);
        int minOffset = buf.getInt(minIndex * Ints.BYTES);

        if (comparator.compare(minOffset, offset) < 0) {
          buf.putInt(pos * Ints.BYTES, minOffset);
          buf.putInt(minIndex * Ints.BYTES, offset);
          if (heapIndexUpdater != null) {
            heapIndexUpdater.updateHeapIndexForOffset(minOffset, pos);
            heapIndexUpdater.updateHeapIndexForOffset(offset, minIndex);
          }

          int parent = getParentIndex(minIndex);
          int parentOffset = buf.getInt(parent * Ints.BYTES);

          if (comparator.compare(offset, parentOffset) > 0) {
            buf.putInt(minIndex * Ints.BYTES, parentOffset);
            buf.putInt(parent * Ints.BYTES, offset);
            if (heapIndexUpdater != null) {
              heapIndexUpdater.updateHeapIndexForOffset(offset, parent);
              heapIndexUpdater.updateHeapIndexForOffset(parentOffset, minIndex);
            }
          }
          minChild = findMinChild(comparator, minIndex);
        }
        pos = minIndex;
      } else {
        int offset = buf.getInt(pos * Ints.BYTES);
        int minOffset = buf.getInt(minIndex * Ints.BYTES);
        if (comparator.compare(minOffset, offset) < 0) {
          buf.putInt(pos * Ints.BYTES, minOffset);
          buf.putInt(minIndex * Ints.BYTES, offset);
          if (heapIndexUpdater != null) {
            heapIndexUpdater.updateHeapIndexForOffset(offset, minIndex);
            heapIndexUpdater.updateHeapIndexForOffset(minOffset, pos);
          }
        }
        break;
      }
    }
  }

  private boolean isEvenLevel(int index)
  {
    int oneBased = index + 1;
    return (oneBased & EVEN_POWERS_OF_TWO) > (oneBased & ODD_POWERS_OF_TWO);
  }

  /**
   * Returns the index of minimum value between {@code index} and
   * {@code index + len}, or {@code -1} if {@code index} is greater than
   * {@code size}.
   */
  private int findMin(Comparator comparator, int index, int len)
  {
    if (index >= heapSize) {
      return -1;
    }
    int limit = Math.min(index, heapSize - len) + len;
    int minIndex = index;
    for (int i = index + 1; i < limit; i++) {
      if (comparator.compare(buf.getInt(i * Ints.BYTES), buf.getInt(minIndex * Ints.BYTES)) < 0) {
        minIndex = i;
      }
    }
    return minIndex;
  }

  /**
   * Returns the minimum child or {@code -1} if no child exists.
   */
  private int findMinChild(Comparator comparator, int index)
  {
    return findMin(comparator, getLeftChildIndex(index), 2);
  }

  /**
   * Returns the minimum grand child or -1 if no grand child exists.
   */
  private int findMinGrandChild(Comparator comparator, int index)
  {
    int leftChildIndex = getLeftChildIndex(index);
    if (leftChildIndex < 0) {
      return -1;
    }
    return findMin(comparator, getLeftChildIndex(leftChildIndex), 4);
  }

  private int getLeftChildIndex(int i)
  {
    return i * 2 + 1;
  }

  private int getRightChildIndex(int i)
  {
    return i * 2 + 2;
  }

  private int getParentIndex(int i)
  {
    if (i == 0) {
      return -1;
    }
    return (i - 1) / 2;
  }

  private int getGrandparentIndex(int i)
  {
    if (i < 3) {
      return -1;
    }
    return (i - 3) / 4;
  }

  /**
   * Returns the index of the max element.
   */
  private int findMaxElementIndex()
  {
    switch (heapSize) {
      case 1:
        return 0; // The lone element in the queue is the maximum.
      case 2:
        return 1; // The lone element in the maxHeap is the maximum.
      default:
        // The max element must sit on the first level of the maxHeap. It is
        // actually the *lesser* of the two from the maxHeap's perspective.
        int offset1 = buf.getInt(1 * Ints.BYTES);
        int offset2 = buf.getInt(2 * Ints.BYTES);
        return maxComparator.compare(offset1, offset2) <= 0 ? 1 : 2;
    }
  }

  @VisibleForTesting
  boolean isIntact()
  {
    for (int i = 0; i < heapSize; i++) {
      if (!verifyIndex(i)) {
        return false;
      }
    }
    return true;
  }

  private boolean verifyIndex(int i)
  {
    Comparator comparator = isEvenLevel(i) ? minComparator : maxComparator;
    int offset = buf.getInt(i * Ints.BYTES);

    int lcIdx = getLeftChildIndex(i);
    if (lcIdx < heapSize) {
      int leftChildOffset = buf.getInt(lcIdx * Ints.BYTES);
      if (comparator.compare(offset, leftChildOffset) > 0) {
        throw new ISE("Left child val[%d] at idx[%d] is less than val[%d] at idx[%d]",
                      leftChildOffset, lcIdx, offset, i);
      }
    }

    int rcIdx = getRightChildIndex(i);
    if (rcIdx < heapSize) {
      int rightChildOffset = buf.getInt(rcIdx * Ints.BYTES);
      if (comparator.compare(offset, rightChildOffset) > 0) {
        throw new ISE("Right child val[%d] at idx[%d] is less than val[%d] at idx[%d]",
                      rightChildOffset, rcIdx, offset, i);
      }
    }

    if (i > 0) {
      int parentIdx = getParentIndex(i);
      int parentOffset = buf.getInt(parentIdx * Ints.BYTES);
      if (comparator.compare(offset, parentOffset) > 0) {
        throw new ISE("Parent val[%d] at idx[%d] is less than val[%d] at idx[%d]",
                      parentOffset, parentIdx, offset, i);
      }
    }

    if (i > 2) {
      int gpIdx = getGrandparentIndex(i);
      int gpOffset = buf.getInt(gpIdx * Ints.BYTES);
      if (comparator.compare(gpOffset, offset) > 0) {
        throw new ISE("Grandparent val[%d] at idx[%d] is less than val[%d] at idx[%d]",
                      gpOffset, gpIdx, offset, i);
      }
    }

    return true;
  }

  @Override
  public String toString()
  {
    if (heapSize == 0) {
      return "[]";
    }

    String ret = "[";
    for (int i = 0; i < heapSize; i++) {
      ret += buf.getInt(i * Ints.BYTES);
      if (i < heapSize - 1) {
        ret += ", ";
      }
    }

    ret += "]";
    return ret;
  }
}
