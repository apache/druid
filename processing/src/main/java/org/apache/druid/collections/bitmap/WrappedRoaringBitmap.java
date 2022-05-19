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
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.nio.ByteBuffer;

public class WrappedRoaringBitmap implements MutableBitmap
{
  // attempt to compress long runs prior to serialization (requires RoaringBitmap version 0.5 or better)
  // this may improve compression greatly in some cases at the expense of slower serialization
  // in the worst case.
  private final boolean compressRunOnSerialization;
  /**
   * Underlying bitmap.
   */
  private RoaringBitmapWriter<MutableRoaringBitmap> writer;

  /**
   * Creates a new WrappedRoaringBitmap wrapping an empty MutableRoaringBitmap
   */
  public WrappedRoaringBitmap()
  {
    this(RoaringBitmapFactory.DEFAULT_COMPRESS_RUN_ON_SERIALIZATION);
  }

  /**
   * Creates a new WrappedRoaringBitmap wrapping an empty MutableRoaringBitmap
   *
   * @param compressRunOnSerialization indicates whether to call {@link RoaringBitmap#runOptimize()} before serializing
   */
  public WrappedRoaringBitmap(boolean compressRunOnSerialization)
  {
    this.writer = RoaringBitmapWriter.bufferWriter().get();
    this.compressRunOnSerialization = compressRunOnSerialization;
  }

  @VisibleForTesting
  public ImmutableBitmap toImmutableBitmap()
  {
    MutableRoaringBitmap bitmap = writer.get().clone();
    if (compressRunOnSerialization) {
      bitmap.runOptimize();
    }
    return new WrappedImmutableRoaringBitmap(bitmap.toImmutableRoaringBitmap());
  }

  @Override
  public byte[] toBytes()
  {
    try {
      MutableRoaringBitmap bitmap = writer.get();
      if (compressRunOnSerialization) {
        bitmap.runOptimize();
      }
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
    this.writer.reset();
  }

  @Override
  public void or(MutableBitmap mutableBitmap)
  {
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) mutableBitmap;
    MutableRoaringBitmap unwrappedOtherBitmap = other.writer.get();
    writer.get().or(unwrappedOtherBitmap);
  }


  @Override
  public int getSizeInBytes()
  {
    MutableRoaringBitmap bitmap = writer.get();
    if (compressRunOnSerialization) {
      bitmap.runOptimize();
    }
    return bitmap.serializedSizeInBytes();
  }

  @Override
  public void add(int entry)
  {
    writer.add(entry);
  }

  @Override
  public int size()
  {
    return writer.get().getCardinality();
  }

  public void serialize(ByteBuffer buffer)
  {
    MutableRoaringBitmap bitmap = writer.get();
    if (compressRunOnSerialization) {
      bitmap.runOptimize();
    }
    bitmap.serialize(buffer);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + writer.getUnderlying();
  }

  @Override
  public void remove(int entry)
  {
    writer.get().remove(entry);
  }

  @Override
  public IntIterator iterator()
  {
    return writer.get().getIntIterator();
  }

  @Override
  public PeekableIntIterator peekableIterator()
  {
    return writer.get().getIntIterator();
  }

  @Override
  public boolean isEmpty()
  {
    return writer.get().isEmpty();
  }

  @Override
  public ImmutableBitmap intersection(ImmutableBitmap otherBitmap)
  {
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) otherBitmap;
    MutableRoaringBitmap unwrappedOtherBitmap = other.writer.get();
    return new WrappedImmutableRoaringBitmap(MutableRoaringBitmap.and(writer.get(), unwrappedOtherBitmap));
  }

  @Override
  public boolean get(int value)
  {
    return writer.get().contains(value);
  }
}
