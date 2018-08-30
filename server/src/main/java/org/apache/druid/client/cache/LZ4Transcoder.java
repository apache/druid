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

package org.apache.druid.client.cache;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.spy.memcached.transcoders.SerializingTranscoder;

import java.nio.ByteBuffer;

public class LZ4Transcoder extends SerializingTranscoder
{

  private final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();

  public LZ4Transcoder()
  {
    super();
  }

  public LZ4Transcoder(int max)
  {
    super(max);
  }

  @Override
  protected byte[] compress(byte[] in)
  {
    if (in == null) {
      throw new NullPointerException("Can't compress null");
    }

    LZ4Compressor compressor = lz4Factory.fastCompressor();

    byte[] out = new byte[compressor.maxCompressedLength(in.length)];
    int compressedLength = compressor.compress(in, 0, in.length, out, 0);

    getLogger().debug("Compressed %d bytes to %d", in.length, compressedLength);

    return ByteBuffer.allocate(Integer.BYTES + compressedLength)
                     .putInt(in.length)
                     .put(out, 0, compressedLength)
                     .array();
  }

  @Override
  protected byte[] decompress(byte[] in)
  {
    byte[] out = null;
    if (in != null) {
      LZ4FastDecompressor decompressor = lz4Factory.fastDecompressor();

      int size = ByteBuffer.wrap(in).getInt();

      out = new byte[size];
      decompressor.decompress(in, Integer.BYTES, out, 0, out.length);
    }
    return out == null ? null : out;
  }
}
