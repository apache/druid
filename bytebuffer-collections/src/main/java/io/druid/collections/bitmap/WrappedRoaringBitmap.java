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

package io.druid.collections.bitmap;

import com.google.common.base.Throwables;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
  private MutableRoaringBitmap bitmap;

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
    this.bitmap = new MutableRoaringBitmap();
    this.compressRunOnSerialization = compressRunOnSerialization;
  }

  ImmutableBitmap toImmutableBitmap()
  {
    MutableRoaringBitmap mrb = bitmap.clone();
    if (compressRunOnSerialization) {
      mrb.runOptimize();
    }
    return new WrappedImmutableRoaringBitmap(mrb);
  }

  @Override
  public byte[] toBytes()
  {
    try {
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      if (compressRunOnSerialization) {
        bitmap.runOptimize();
      }
      bitmap.serialize(new DataOutputStream(out));
      return out.toByteArray();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public int compareTo(ImmutableBitmap other)
  {
    return 0;
  }

  @Override
  public void clear()
  {
    this.bitmap.clear();
  }

  @Override
  public void or(MutableBitmap mutableBitmap)
  {
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) mutableBitmap;
    MutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    bitmap.or(unwrappedOtherBitmap);
  }

  @Override
  public void and(MutableBitmap mutableBitmap)
  {
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) mutableBitmap;
    MutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    bitmap.and(unwrappedOtherBitmap);
  }


  @Override
  public void andNot(MutableBitmap mutableBitmap)
  {
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) mutableBitmap;
    MutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    bitmap.andNot(unwrappedOtherBitmap);
  }


  @Override
  public void xor(MutableBitmap mutableBitmap)
  {
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) mutableBitmap;
    MutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    bitmap.xor(unwrappedOtherBitmap);
  }

  @Override
  public int getSizeInBytes()
  {
    if (compressRunOnSerialization) {
      bitmap.runOptimize();
    }
    return bitmap.serializedSizeInBytes();
  }

  @Override
  public void add(int entry)
  {
    bitmap.add(entry);
  }

  @Override
  public int size()
  {
    return bitmap.getCardinality();
  }

  @Override
  public void serialize(ByteBuffer buffer)
  {
    if (compressRunOnSerialization) {
      bitmap.runOptimize();
    }
    try {
      bitmap.serialize(
          new DataOutputStream(
              new OutputStream()
              {
                ByteBuffer mBB;

                OutputStream init(ByteBuffer mbb)
                {
                  mBB = mbb;
                  return this;
                }

                @Override
                public void close()
                {
                  // unnecessary
                }

                @Override
                public void flush()
                {
                  // unnecessary
                }

                @Override
                public void write(int b)
                {
                  mBB.put((byte) b);
                }

                @Override
                public void write(byte[] b)
                {
                  mBB.put(b);
                }

                @Override
                public void write(byte[] b, int off, int l)
                {
                  mBB.put(b, off, l);
                }
              }.init(buffer)
          )
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e); // impossible in theory
    }
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + bitmap.toString();
  }

  @Override
  public void remove(int entry)
  {
    bitmap.remove(entry);
  }

  @Override
  public IntIterator iterator()
  {
    return bitmap.getIntIterator();
  }

  @Override
  public boolean isEmpty()
  {
    return bitmap.isEmpty();
  }

  @Override
  public ImmutableBitmap union(ImmutableBitmap otherBitmap)
  {
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) otherBitmap;
    MutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    return new WrappedImmutableRoaringBitmap(MutableRoaringBitmap.or(bitmap, unwrappedOtherBitmap));
  }

  @Override
  public ImmutableBitmap intersection(ImmutableBitmap otherBitmap)
  {
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) otherBitmap;
    MutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    return new WrappedImmutableRoaringBitmap(MutableRoaringBitmap.and(bitmap, unwrappedOtherBitmap));
  }

  @Override
  public ImmutableBitmap difference(ImmutableBitmap otherBitmap)
  {
    WrappedRoaringBitmap other = (WrappedRoaringBitmap) otherBitmap;
    MutableRoaringBitmap unwrappedOtherBitmap = other.bitmap;
    return new WrappedImmutableRoaringBitmap(MutableRoaringBitmap.andNot(bitmap, unwrappedOtherBitmap));
  }

  @Override
  public boolean get(int value)
  {
    return bitmap.contains(value);
  }
}
