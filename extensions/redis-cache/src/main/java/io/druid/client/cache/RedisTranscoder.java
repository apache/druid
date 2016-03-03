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

package io.druid.client.cache;

import com.google.common.primitives.Ints;
import com.metamx.common.logger.Logger;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;

public class RedisTranscoder
{
  private static final Logger log = new Logger(RedisTranscoder.class);
  private final LZ4Factory factory;

  public RedisTranscoder()
  {
    factory = LZ4Factory.fastestInstance();
  }

  public byte[] compress(byte[] in)
  {
    if (in == null) {
      throw new NullPointerException("Can't compress null");
    }

    final LZ4Compressor compressor = factory.fastCompressor();
    final byte[] out = new byte[compressor.maxCompressedLength(in.length)];
    final int compressedLength = compressor.compress(in, 0, in.length, out, 0);

    return ByteBuffer.allocate(Ints.BYTES + compressedLength)
                     .putInt(in.length)
                     .put(out, 0, compressedLength)
                     .array();
  }

  public byte[] decompress(byte[] in)
  {
    final byte[] out;
    if (in != null) {
      final LZ4FastDecompressor decompressor = factory.fastDecompressor();
      final int size = ByteBuffer.wrap(in).getInt();

      out = new byte[size];
      decompressor.decompress(in, Ints.BYTES, out, 0, out.length);
    } else {
      out = null;
    }
    return out;
  }
}
