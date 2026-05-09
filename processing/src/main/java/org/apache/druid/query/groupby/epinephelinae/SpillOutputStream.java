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

package org.apache.druid.query.groupby.epinephelinae;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * OutputStream that starts buffering in a heap byte array and switches to a disk file via
 * {@link LimitedTemporaryStorage} once the written bytes exceed the threshold. This avoids
 * the createFile/delete round-trip for small spills while bounding peak extra heap to the
 * threshold size.
 */
public class SpillOutputStream extends OutputStream
{
  private static final int INITIAL_BUFFER_SIZE = 4096;

  private final LimitedTemporaryStorage temporaryStorage;
  private final long threshold;
  private ByteArrayOutputStream memoryBuffer;
  private LimitedTemporaryStorage.LimitedOutputStream fileOut;
  private boolean thresholdExceeded;

  SpillOutputStream(LimitedTemporaryStorage temporaryStorage, long threshold)
  {
    this.temporaryStorage = temporaryStorage;
    this.threshold = threshold;
    this.memoryBuffer = new ByteArrayOutputStream((int) Math.min(threshold, INITIAL_BUFFER_SIZE));
  }

  @Override
  public void write(int b) throws IOException
  {
    checkThreshold(1);
    if (fileOut != null) {
      fileOut.write(b);
    } else {
      memoryBuffer.write(b);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException
  {
    checkThreshold(len);
    if (fileOut != null) {
      fileOut.write(b, off, len);
    } else {
      memoryBuffer.write(b, off, len);
    }
  }

  @Override
  public void flush() throws IOException
  {
    if (fileOut != null) {
      fileOut.flush();
    }
  }

  @Override
  public void close() throws IOException
  {
    if (fileOut != null) {
      fileOut.close();
    }
  }

  boolean isInMemory()
  {
    return fileOut == null;
  }

  byte[] toByteArray()
  {
    return memoryBuffer.toByteArray();
  }

  File getFile()
  {
    return fileOut.getFile();
  }

  private void checkThreshold(int count) throws IOException
  {
    if (!thresholdExceeded && memoryBuffer.size() + count > threshold) {
      thresholdExceeded = true;
      switchToDisk();
    }
  }

  private void switchToDisk() throws IOException
  {
    final LimitedTemporaryStorage.LimitedOutputStream out = temporaryStorage.createFile();
    try {
      memoryBuffer.writeTo(out);
    }
    catch (IOException e) {
      out.close();
      throw e;
    }
    fileOut = out;
    memoryBuffer = null;
  }
}
