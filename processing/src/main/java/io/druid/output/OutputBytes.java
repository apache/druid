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

package io.druid.output;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.WritableByteChannel;

/**
 * Appendable byte sequence for temporary storage. Methods inherited from {@link OutputStream}, {@link
 * WritableByteChannel} and {@link #writeInt(int)} append to the sequence. Methods {@link
 * #writeTo(WritableByteChannel)} and {@link #asInputStream()} allow to write the sequence somewhere else.
 *
 * OutputBytes is a resource that is managed by {@link OutputMedium}, so it's own {@link #close()} method does nothing.
 * However OutputBytes should appear closed, i. e. {@link #isOpen()} returns false, after the parental OutputMedium is
 * closed.
 */
public abstract class OutputBytes extends OutputStream implements WritableByteChannel
{
  /**
   * Writes 4 bytes of the given value in big-endian order, i. e. similar to {@link java.io.DataOutput#writeInt(int)}.
   */
  public abstract void writeInt(int v) throws IOException;

  /**
   * Returns the number of bytes written to this OutputBytes so far.
   */
  public abstract long size() throws IOException;

  /**
   * Takes all bytes that are written to this OutputBytes so far and writes them into the given channel.
   */
  public abstract void writeTo(WritableByteChannel channel) throws IOException;

  /**
   * Creates a finite {@link InputStream} with the bytes that are written to this OutputBytes so far. The returned
   * InputStream must be closed properly after it's used up.
   */
  public abstract InputStream asInputStream() throws IOException;

  /**
   * @deprecated does nothing.
   */
  @Deprecated
  @Override
  public final void close()
  {
    // Does nothing.
  }
}
