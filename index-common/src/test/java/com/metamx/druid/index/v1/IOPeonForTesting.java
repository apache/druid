package com.metamx.druid.index.v1;

import com.google.common.collect.Maps;
import com.metamx.druid.kv.IOPeon;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
*/
class IOPeonForTesting implements IOPeon
{
  Map<String, ByteArrayOutputStream> outStreams = Maps.newHashMap();

  @Override
  public OutputStream makeOutputStream(String filename) throws IOException
  {
    ByteArrayOutputStream stream = outStreams.get(filename);

    if (stream == null) {
      stream = new ByteArrayOutputStream();
      outStreams.put(filename, stream);
    }

    return stream;
  }

  @Override
  public InputStream makeInputStream(String filename) throws IOException
  {
    ByteArrayOutputStream outStream = outStreams.get(filename);

    if (outStream == null) {
      throw new FileNotFoundException(String.format("unknown file[%s]", filename));
    }

    return new ByteArrayInputStream(outStream.toByteArray());
  }

  @Override
  public void cleanup() throws IOException
  {
    outStreams.clear();
  }
}
