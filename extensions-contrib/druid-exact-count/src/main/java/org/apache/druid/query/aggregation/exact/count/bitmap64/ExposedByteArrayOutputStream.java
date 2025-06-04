package org.apache.druid.query.aggregation.exact.count.bitmap64;

import java.io.ByteArrayOutputStream;

/**
 * This class is used to expose the underlying byte array in the output stream, to prevent extra copying of
 * the array.
 */
public class ExposedByteArrayOutputStream extends ByteArrayOutputStream
{
  byte[] getBuffer()
  {
    return this.buf;
  }
}
