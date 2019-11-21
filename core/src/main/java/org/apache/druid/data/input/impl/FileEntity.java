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

package org.apache.druid.data.input.impl;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.utils.CompressionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

public class FileEntity implements InputEntity
{
  private final File file;

  FileEntity(File file)
  {
    this.file = file;
  }

  @Override
  public CleanableFile fetch(File temporaryDirectory, byte[] fetchBuffer)
  {
    return new CleanableFile()
    {
      @Override
      public File file()
      {
        return file;
      }

      @Override
      public void close()
      {
        // do nothing
      }
    };
  }

  @Override
  public URI getUri()
  {
    return file.toURI();
  }

  @Override
  public InputStream open(long offset) throws IOException
  {
    final Closer closer = Closer.create();
    final FileInputStream stream = closer.register(new FileInputStream(file));
    final FileChannel channel = closer.register(stream.getChannel());
    channel.position(offset);
    return CompressionUtils.decompress(
        new InputStreamWithBaggage(Channels.newInputStream(channel), closer),
        file.getName()
    );
  }

  @Override
  public Predicate<Throwable> getRetryCondition()
  {
    return Predicates.alwaysFalse();
  }

  private static class InputStreamWithBaggage extends InputStream
  {
    private final InputStream delegate;
    private final Closer closer;

    private InputStreamWithBaggage(InputStream delegate, Closer closer)
    {
      this.delegate = delegate;
      this.closer = closer;
      closer.register(delegate);
    }

    @Override
    public int read() throws IOException
    {
      return delegate.read();
    }

    @Override
    public int read(byte b[]) throws IOException
    {
      return delegate.read(b);
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException
    {
      return delegate.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException
    {
      return delegate.skip(n);
    }

    @Override
    public int available() throws IOException
    {
      return delegate.available();
    }

    @Override
    public void close() throws IOException
    {
      closer.close();
    }
  }
}
