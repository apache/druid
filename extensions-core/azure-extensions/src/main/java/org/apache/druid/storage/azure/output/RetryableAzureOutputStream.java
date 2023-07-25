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

package org.apache.druid.storage.azure.output;

import com.google.common.io.CountingOutputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.azure.AzureStorage;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

public class RetryableAzureOutputStream extends OutputStream
{

  private static final Logger log = new Logger(RetryableAzureOutputStream.class);

  private boolean error = false;
  private boolean closed = false;

  private final AzureStorage azureStorage;
  private final AzureOutputConfig config;
  private final File chunkStorePath;
  private final long chunkSize;

  private Chunk currentChunk;
  private int nextChunkId = 1;
  private int numChunksPushed;

  public RetryableAzureOutputStream(
      AzureStorage azureStorage,
      AzureOutputConfig config
  )
  {
    this.azureStorage = azureStorage;
    this.config = config;
  }

  @Override
  public void write(int b) throws IOException
  {

  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException
  {
    super.write(b, off, len);
  }

  @Override
  public void flush() throws IOException
  {
    super.flush();
  }

  @Override
  public void close() throws IOException
  {
    if (closed) {
      return;
    }

    closed = true;
    Closer closer = Closer.create();
  }

  private static class Chunk implements Closeable
  {
    private final int id;
    private final File file;
    private final CountingOutputStream outputStream;
    private boolean closed = false;

    private Chunk(int id, File file) throws FileNotFoundException
    {
      this.id = id;
      this.file = file;
      this.outputStream = new CountingOutputStream(new FastBufferedOutputStream(new FileOutputStream(file)));
    }

    private long length()
    {
      return outputStream.getCount();
    }

    private boolean delete()
    {
      return file.delete();
    }

    private String absolutePath()
    {
      return file.getAbsolutePath();
    }

    @Override
    public void close() throws IOException
    {
      if (closed) {
        return;
      }
      closed = true;
      outputStream.close();
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Chunk chunk = (Chunk) o;
      return id == chunk.id;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(id);
    }

    @Override
    public String toString()
    {
      return "Chunk{" +
             "id=" + id +
             ", file=" + absolutePath() +
             ", length=" + length() +
             '}';
    }
  }
}
