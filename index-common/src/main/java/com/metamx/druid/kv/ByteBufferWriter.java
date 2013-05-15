package com.metamx.druid.kv;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.InputSupplier;
import com.google.common.primitives.Ints;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 */
public class ByteBufferWriter<T> implements Closeable
{
  private final IOPeon ioPeon;
  private final String filenameBase;
  private final ObjectStrategy<T> strategy;

  private CountingOutputStream headerOut = null;
  private CountingOutputStream valueOut = null;

  public ByteBufferWriter(
      IOPeon ioPeon,
      String filenameBase,
      ObjectStrategy<T> strategy
  )
  {
    this.ioPeon = ioPeon;
    this.filenameBase = filenameBase;
    this.strategy = strategy;
  }

  public void open() throws IOException
  {
    headerOut = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("header")));
    valueOut = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("value")));
  }

  public void write(T objectToWrite) throws IOException
  {
    byte[] bytesToWrite = strategy.toBytes(objectToWrite);

    headerOut.write(Ints.toByteArray(bytesToWrite.length));
    valueOut.write(bytesToWrite);
  }

  private String makeFilename(String suffix)
  {
    return String.format("%s.%s", filenameBase, suffix);
  }

  @Override
  public void close() throws IOException
  {
    headerOut.close();
    valueOut.close();

    final long numBytesWritten = headerOut.getCount() + valueOut.getCount();
    Preconditions.checkState(
        numBytesWritten < Integer.MAX_VALUE, "Wrote[%s] bytes, which is too many.", numBytesWritten
    );
  }

  public InputSupplier<InputStream> combineStreams()
  {
    return ByteStreams.join(
        Iterables.transform(
            Arrays.asList("header", "value"),
            new Function<String, InputSupplier<InputStream>>()
            {
              @Override
              public InputSupplier<InputStream> apply(final String input)
              {
                return new InputSupplier<InputStream>()
                {
                  @Override
                  public InputStream getInput() throws IOException
                  {
                    return ioPeon.makeInputStream(makeFilename(input));
                  }
                };
              }
            }
        )
    );
  }
}
