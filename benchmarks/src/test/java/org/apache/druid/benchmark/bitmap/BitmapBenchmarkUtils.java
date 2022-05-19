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

package org.apache.druid.benchmark.bitmap;

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.WrappedImmutableConciseBitmap;
import org.apache.druid.collections.bitmap.WrappedImmutableRoaringBitmap;
import org.apache.druid.extendedset.intset.ImmutableConciseSet;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class BitmapBenchmarkUtils
{
  private static final Logger LOG = new Logger(BitmapBenchmarkUtils.class);

  public static ImmutableBitmap toOffheap(ImmutableBitmap bitmap) throws IOException
  {
    if (bitmap instanceof WrappedImmutableConciseBitmap) {
      final WrappedImmutableConciseBitmap conciseBitmap = (WrappedImmutableConciseBitmap) bitmap;
      final byte[] bytes = conciseBitmap.getBitmap().toBytes();
      final ByteBuffer buf = ByteBuffer.allocateDirect(bytes.length).put(bytes);
      buf.rewind();
      return new WrappedImmutableConciseBitmap(new ImmutableConciseSet(buf.asIntBuffer()));
    } else if (bitmap instanceof WrappedImmutableRoaringBitmap) {
      final WrappedImmutableRoaringBitmap roaringBitmap = (WrappedImmutableRoaringBitmap) bitmap;
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      roaringBitmap.getBitmap().serialize(new DataOutputStream(out));
      final byte[] bytes = out.toByteArray();
      final ByteBuffer buf = ByteBuffer.allocateDirect(bytes.length);
      buf.put(bytes);
      buf.rewind();
      return new WrappedImmutableRoaringBitmap(new ImmutableRoaringBitmap(buf.asReadOnlyBuffer()));
    } else {
      throw new IAE("Unsupported bitmap type [%s]", bitmap.getClass().getSimpleName());
    }
  }

  public static void printSizeStats(String type, double density, long count, long totalBytes)
  {
    LOG.info(
        StringUtils.format(
            " type = %s, density = %06.5f, count = %5d, average byte size = %5d",
            type,
            density,
            count,
            totalBytes / count
        )
    );
  }

  private BitmapBenchmarkUtils()
  {
  }
}
