/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.client.cache;

import com.google.common.primitives.Ints;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Decompressor;
import net.jpountz.lz4.LZ4Factory;
import net.spy.memcached.transcoders.SerializingTranscoder;

import java.nio.ByteBuffer;

public class LZ4Transcoder extends SerializingTranscoder
{

  private final LZ4Factory lz4Factory;

  public LZ4Transcoder()
  {
    super();
    lz4Factory = LZ4Factory.fastestJavaInstance();
  }

  public LZ4Transcoder(int max)
  {
    super(max);
    lz4Factory = LZ4Factory.fastestJavaInstance();
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

    return ByteBuffer.allocate(Ints.BYTES + compressedLength)
                     .putInt(in.length)
                     .put(out, 0, compressedLength)
                     .array();
  }

  @Override
  protected byte[] decompress(byte[] in)
  {
    byte[] out = null;
    if(in != null) {
      LZ4Decompressor decompressor = lz4Factory.decompressor();

      int size = ByteBuffer.wrap(in).getInt();

      out = new byte[size];
      decompressor.decompress(in, Ints.BYTES, out, 0, out.length);
    }
    return out == null ? null : out;
  }
}
