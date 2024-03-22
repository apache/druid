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

import com.google.common.annotations.VisibleForTesting;
import it.unimi.dsi.fastutil.ints.IntIterators;
import it.unimi.dsi.fastutil.ints.IntListIterator;
import org.apache.druid.extendedset.intset.EmptyIntIterator;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class WrappedRoaringBitmap implements MutableBitmap
{
  private static final int ARRAY_SIZE = 4;
  private static final int NOT_SET = -1;

  /**
   * Underlying bitmap.
   */
  @Nullable
  private RoaringBitmapWriter<MutableRoaringBitmap> writer;

  /**
   * Array used instead of {@link #writer} when the number of distinct values is less than {@link #ARRAY_SIZE}.
   * Saves memory vs. a full {@link RoaringBitmapWriter}.
   */
  @Nullable
  private int[] smallArray;

  /**
   * Creates a new, empty bitmap.
   */
  public WrappedRoaringBitmap()
  {
  }

  @VisibleForTesting
  public ImmutableBitmap toImmutableBitmap()
  {
    initializeWriterIfNeeded();
    MutableRoaringBitmap bitmap = writer.get().clone();
    bitmap.runOptimize();
    return new WrappedImmutableRoaringBitmap(bitmap.toImmutableRoaringBitmap());
  }

  @Override
  public byte[] toBytes()
  {
    initializeWriterIfNeeded();
    try {
      MutableRoaringBitmap bitmap = writer.get();
      bitmap.runOptimize();
      ByteBuffer buffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes());
      bitmap.serialize(buffer);
      return buffer.array();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear()
  {
    this.writer = null;
    this.smallArray = null;
  }

  @Override
  public void or(MutableBitmap mutableBitmap)
  {
    initializeWriterIfNeeded();
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) mutableBitmap;
    other.initializeWriterIfNeeded();
    MutableRoaringBitmap unwrappedOtherBitmap = other.writer.get();
    writer.get().or(unwrappedOtherBitmap);
  }

  @Override
  public int getSizeInBytes()
  {
    initializeWriterIfNeeded();
    MutableRoaringBitmap bitmap = writer.get();
    bitmap.runOptimize();
    return bitmap.serializedSizeInBytes();
  }

  @Override
  public void add(int entry)
  {
    if (entry < 0) {
      throw new IllegalArgumentException("Cannot add negative ints");
    } else if (writer != null) {
      writer.add(entry);
    } else {
      if (smallArray == null) {
        smallArray = new int[ARRAY_SIZE];
        Arrays.fill(smallArray, NOT_SET);
      }

      for (int i = 0; i < smallArray.length; i++) {
        if (smallArray[i] == NOT_SET) {
          if (i > 0 && entry <= smallArray[i - 1]) {
            // Can't handle nonascending order with smallArray
            break;
          }

          smallArray[i] = entry;
          return;
        }
      }

      initializeWriterIfNeeded();
      smallArray = null;
      writer.add(entry);
    }
  }

  @Override
  public int size()
  {
    if (writer != null) {
      return writer.get().getCardinality();
    } else if (smallArray != null) {
      for (int i = 0; i < smallArray.length; i++) {
        if (smallArray[i] == NOT_SET) {
          return i;
        }
      }

      return ARRAY_SIZE;
    } else {
      return 0;
    }
  }

  public void serialize(ByteBuffer buffer)
  {
    initializeWriterIfNeeded();
    MutableRoaringBitmap bitmap = writer.get();
    bitmap.runOptimize();
    bitmap.serialize(buffer);
  }

  @Override
  public String toString()
  {
    if (writer != null) {
      return getClass().getSimpleName() + writer.getUnderlying();
    } else if (smallArray != null) {
      return getClass().getSimpleName() + Arrays.toString(smallArray);
    } else {
      return getClass().getSimpleName() + "[]";
    }
  }

  @Override
  public void remove(int entry)
  {
    initializeWriterIfNeeded();
    writer.get().remove(entry);
  }

  @Override
  public IntIterator iterator()
  {
    if (writer != null) {
      return writer.get().getIntIterator();
    } else if (smallArray != null) {
      final int sz = size();

      class SmallArrayIterator implements IntIterator
      {
        private final IntListIterator iterator;

        public SmallArrayIterator(IntListIterator iterator)
        {
          this.iterator = iterator;
        }

        @Override
        public IntIterator clone()
        {
          return new SmallArrayIterator(IntIterators.wrap(smallArray, 0, sz));
        }

        @Override
        public boolean hasNext()
        {
          return iterator.hasNext();
        }

        @Override
        public int next()
        {
          return iterator.nextInt();
        }
      }

      return new SmallArrayIterator(IntIterators.wrap(smallArray, 0, sz));
    } else {
      return EmptyIntIterator.instance();
    }
  }

  @Override
  public PeekableIntIterator peekableIterator()
  {
    if (writer != null) {
      return writer.get().getIntIterator();
    } else {
      return new PeekableIteratorAdapter<>(iterator());
    }
  }

  @Override
  public boolean isEmpty()
  {
    if (writer != null) {
      return writer.get().isEmpty();
    } else {
      return smallArray == null;
    }
  }

  @Override
  public ImmutableBitmap intersection(ImmutableBitmap otherBitmap)
  {
    initializeWriterIfNeeded();
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) otherBitmap;
    other.initializeWriterIfNeeded();
    MutableRoaringBitmap unwrappedOtherBitmap = other.writer.get();
    return new WrappedImmutableRoaringBitmap(MutableRoaringBitmap.and(writer.get(), unwrappedOtherBitmap));
  }

  @Override
  public ImmutableBitmap union(ImmutableBitmap otherBitmap)
  {
    initializeWriterIfNeeded();
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) otherBitmap;
    other.initializeWriterIfNeeded();
    MutableRoaringBitmap unwrappedOtherBitmap = other.writer.get();
    return new WrappedImmutableRoaringBitmap(MutableRoaringBitmap.or(writer.get(), unwrappedOtherBitmap));
  }

  @Override
  public boolean get(int value)
  {
    if (value < 0) {
      return false;
    } else if (writer != null) {
      return writer.get().contains(value);
    } else if (smallArray != null) {
      for (int i : smallArray) {
        if (i == value) {
          return true;
        }
      }

      return false;
    } else {
      return false;
    }
  }

  private void initializeWriterIfNeeded()
  {
    if (writer == null) {
      writer = RoaringBitmapWriter.bufferWriter().get();

      if (smallArray != null) {
        for (int i : smallArray) {
          if (i != NOT_SET) {
            writer.add(i);
          }
        }

        smallArray = null;
      }
    }
  }
}
