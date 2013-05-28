package com.metamx.druid.client.cache;

import net.spy.memcached.transcoders.SerializingTranscoder;
import org.xerial.snappy.Snappy;

import java.io.IOException;

public class SnappyTranscoder extends SerializingTranscoder
{
  public SnappyTranscoder()
  {
    super();
  }

  public SnappyTranscoder(int max)
  {
    super(max);
  }

  @Override
  protected byte[] compress(byte[] in)
  {
    if (in == null) {
      throw new NullPointerException("Can't compress null");
    }

    byte[] out;
    try {
      out = Snappy.compress(in);
    } catch(IOException e) {
      throw new RuntimeException("IO exception compressing data", e);
    }
    getLogger().debug("Compressed %d bytes to %d", in.length, out.length);
    return out;
  }

  @Override
  protected byte[] decompress(byte[] in)
  {
    byte[] out = null;
    if(in != null) {
      try {
        out = Snappy.uncompress(in);
      } catch (IOException e) {
        getLogger().warn("Failed to decompress data", e);
      }
    }
    return out == null ? null : out;
  }
}
