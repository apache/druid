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
package io.druid.segment.store;


import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Implementation class for buffered {@link IndexOutput} that writes to an {@link OutputStream}.
 */
public class OutputStreamIndexOutput extends IndexOutput
{
  private final BufferedOutputStream os;

  private long bytesWritten = 0L;
  private boolean flushedOnClose = false;

  /**
   * @param name
   * @param out
   * @param bufferSize
   */
  public OutputStreamIndexOutput(String name, OutputStream out, int bufferSize)
  {
    super(name);
    this.os = new BufferedOutputStream(out, bufferSize);
  }

  @Override
  public final void writeByte(byte b) throws IOException
  {
    os.write(b);
    bytesWritten++;
  }

  @Override
  public final void writeBytes(byte[] b, int offset, int length) throws IOException
  {
    os.write(b, offset, length);
    bytesWritten += length;
  }

  @Override
  public void close() throws IOException
  {
    try (final OutputStream o = os) {
      if (!flushedOnClose) {
        flushedOnClose = true;
        o.flush();
      }
    }
  }

  @Override
  public final long getFilePointer() throws IOException
  {
    return bytesWritten;
  }

}
