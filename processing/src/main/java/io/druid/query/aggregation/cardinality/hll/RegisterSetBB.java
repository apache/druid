package io.druid.query.aggregation.cardinality.hll;

/*
 * Copyright (C) 2012 Clearspring Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class RegisterSetBB
{
  public final static int LOG2_BITS_PER_WORD = 6;
  public final static int REGISTER_SIZE = 5;

  public static int getBits(int count)
  {
    return count / LOG2_BITS_PER_WORD;
  }

  public static int getSizeForCount(int count)
  {
    int bits = getBits(count);
    if (bits == 0)
    {
      return 1;
    }
    else if (bits % Integer.SIZE == 0)
    {
      return bits;
    }
    else
    {
      return bits + 1;
    }
  }

  private final IntBuffer M;

  public RegisterSetBB(ByteBuffer buffer)
  {
    this.M = buffer.asIntBuffer();
  }

  public void set(int position, int value)
  {
    int bucketPos = position / LOG2_BITS_PER_WORD;
    int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
    this.M.put(bucketPos, (this.M.get(bucketPos) & ~(0x1f << shift)) | (value << shift));
  }

  public int get(int position)
  {
    int bucketPos = position / LOG2_BITS_PER_WORD;
    int shift = REGISTER_SIZE * (position - (bucketPos * LOG2_BITS_PER_WORD));
    return (this.M.get(bucketPos) & (0x1f << shift)) >>> shift;
  }

  public boolean updateIfGreater(int position, int value)
  {
    int bucket = position / LOG2_BITS_PER_WORD;
    int shift  = REGISTER_SIZE * (position - (bucket * LOG2_BITS_PER_WORD));
    int mask = 0x1f << shift;

    // Use long to avoid sign issues with the left-most shift
    int currM = M.get(bucket);
    long curVal = currM & mask;
    long newVal = value << shift;
    if (curVal < newVal) {
      this.M.put(bucket,(int) ((currM & ~mask) | newVal));
      return true;
    } else {
      return false;
    }
  }

  public void merge(RegisterSetBB that)
  {
    for (int bucket = 0; bucket < M.remaining(); bucket++)
    {
      int thisM = this.M.get(bucket);
      int thatM = that.M.get(bucket);

      int this1 = thisM & 0x1f;
      int this2 = thisM & 0x3e0;
      int this3 = thisM & 0x7c00;
      int this4 = thisM & 0xf8000;
      int this5 = thisM & 0x1f00000;
      int this6 = thisM & 0x3e000000;

      int that1 = thatM & 0x1f;
      int that2 = thatM & 0x3e0;
      int that3 = thatM & 0x7c00;
      int that4 = thatM & 0xf8000;
      int that5 = thatM & 0x1f00000;
      int that6 = thatM & 0x3e000000;

      this.M.put(bucket,
          (this1 < that1 ? that1 : this1)
          | (this2 < that2 ? that2 : this2)
          | (this3 < that3 ? that3 : this3)
          | (this4 < that4 ? that4 : this4)
          | (this5 < that5 ? that5 : this5)
          | (this6 < that6 ? that6 : this6)
      );
    }
  }
}