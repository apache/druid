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

package org.apache.druid.collections.bitmap;

import org.roaringbitmap.PeekableCharIterator;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.DruidRoaringBufferAccess;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MappeableContainer;
import org.roaringbitmap.buffer.PointableRoaringArray;

import java.util.NoSuchElementException;

/**
 * A {@link PeekableIntIterator} over an {@link ImmutableRoaringBitmap} that repositions by finding the next container
 * keys with {@link PointableRoaringArray#advanceUntil(char, int)}, which can binary search when appropriate.
 * The builtin iterator from {@link ImmutableRoaringBitmap#getIntIterator()} is slower since it advances containers
 * one at a time.
 */
public final class SeekableRoaringIntIterator implements PeekableIntIterator
{
  private final PointableRoaringArray highLowContainer;
  private final int size;

  /**
   * Index of the container that {@link #iter} is reading.
   */
  private int index;

  /**
   * Key of the container at {@link #index}, shifted into the high bits.
   */
  private int shiftedKey;

  /**
   * Iterator over the (short) values of the current container.
   */
  private PeekableCharIterator iter;

  /**
   * Whether {@link #index} is in range. While true, {@code iter.hasNext()} is also true.
   */
  private boolean ok;

  /**
   * The container behind {@link #index}, retained so that rewinding within it does not have to materialize it again.
   */
  private MappeableContainer container;
  private int containerIndex = -1;

  public SeekableRoaringIntIterator(final ImmutableRoaringBitmap bitmap)
  {
    this.highLowContainer = DruidRoaringBufferAccess.highLowContainer(bitmap);
    this.size = highLowContainer.size();
    setContainer(0);
  }

  /**
   * Positions this iterator so that {@link #peekNext()} returns the smallest value >= target, or {@link #hasNext()}
   * returns false if the bitmap holds no such value. Unlike {@link #advanceIfNeeded}, target may be below the
   * current position.
   */
  public void seek(final int target)
  {
    final int targetKey = target >>> 16;

    if (ok && (shiftedKey >>> 16) == targetKey) {
      if (iter.hasNext() && (iter.peekNext() & 0xFFFF) > (target & 0xFFFF)) {
        // Attempting to seek within a container to a value earlier than the current iter position. Need to rewrap.
        setContainer(index);
      }
    } else if (ok && targetKey > (shiftedKey >>> 16)) {
      // Moving forwards one or more container(s).
      setContainer(highLowContainer.advanceUntil((char) targetKey, index));
    } else {
      // Moving backwards one or more container(s).
      final int found = highLowContainer.getContainerIndex((char) targetKey);
      // A miss returns -(insertionPoint + 1), and the insertion point is the first container above targetKey.
      setContainer(found >= 0 ? found : -found - 1);
    }

    if (ok && (shiftedKey >>> 16) == targetKey) {
      iter.advanceIfNeeded((char) target);
      if (!iter.hasNext()) {
        setContainer(index + 1);
      }
    }
  }

  @Override
  public void advanceIfNeeded(final int minval)
  {
    // Forward only: return early if minval is earlier than current iteration state.
    if (!ok || Integer.compareUnsigned(peekNext(), minval) >= 0) {
      return;
    }
    seek(minval);
  }

  @Override
  public boolean hasNext()
  {
    return ok;
  }

  @Override
  public int next()
  {
    if (!ok) {
      throw new NoSuchElementException();
    }
    final int x = iter.nextAsInt() | shiftedKey;
    if (!iter.hasNext()) {
      setContainer(index + 1);
    }
    return x;
  }

  @Override
  public int peekNext()
  {
    if (!ok) {
      throw new NoSuchElementException();
    }
    return iter.peekNext() | shiftedKey;
  }

  @Override
  public SeekableRoaringIntIterator clone()
  {
    try {
      final SeekableRoaringIntIterator cloned = (SeekableRoaringIntIterator) super.clone();
      if (iter != null) {
        cloned.iter = iter.clone();
      }
      return cloned;
    }
    catch (CloneNotSupportedException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * Seeks to a new container, by index, and resets {@link #iter}.
   */
  private void setContainer(final int newIndex)
  {
    index = newIndex;
    if (newIndex >= 0 && newIndex < size) {
      if (newIndex != containerIndex) {
        container = highLowContainer.getContainerAtIndex(newIndex);
        containerIndex = newIndex;
        shiftedKey = (highLowContainer.getKeyAtIndex(newIndex)) << 16;
      }
      iter = container.getCharIterator();
      ok = iter.hasNext();
    } else {
      iter = null;
      ok = false;
    }
  }
}
