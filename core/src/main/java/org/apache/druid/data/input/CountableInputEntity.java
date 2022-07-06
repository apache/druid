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

package org.apache.druid.data.input;

import com.google.common.base.Predicate;
import com.google.common.io.CountingInputStream;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Can be used to count number of bytes read from the base InputEntity
 */
public class CountableInputEntity implements InputEntity
{
  private final InputStats inputStats;
  private final InputEntity baseInputEntity;

  public CountableInputEntity(InputEntity baseInputEntity, InputStats inputStats)
  {
    this.baseInputEntity = baseInputEntity;
    this.inputStats = inputStats;
  }

  @Nullable
  @Override
  public URI getUri()
  {
    return baseInputEntity.getUri();
  }

  @Override
  public InputStream open() throws IOException
  {
    return new BytesCountingInputStream(baseInputEntity.open(), inputStats);
  }

  @Override
  public CleanableFile fetch(File temporaryDirectory, byte[] fetchBuffer) throws IOException
  {
    final CleanableFile cleanableFile = baseInputEntity.fetch(temporaryDirectory, fetchBuffer);
    inputStats.incrementProcessedBytes(cleanableFile.file().length());
    return cleanableFile;
  }

  @Override
  public Predicate<Throwable> getRetryCondition()
  {
    return baseInputEntity.getRetryCondition();
  }

  @Override
  public InputEntity getBaseInputEntity()
  {
    return baseInputEntity;
  }

  static class BytesCountingInputStream extends FilterInputStream
  {
    private final InputStats inputStats;

    /**
     * Wraps another input stream, counting the number of bytes read.
     * <p>
     * Similar to {@link CountingInputStream} but does not reset count on call to
     * {@link CountingInputStream#reset()}
     *
     * @param in the input stream to be wrapped
     */
    public BytesCountingInputStream(@Nullable InputStream in, InputStats inputStats)
    {
      super(in);
      this.inputStats = inputStats;
    }

    @Override
    public int read() throws IOException
    {
      int result = in.read();
      if (result != -1) {
        inputStats.incrementProcessedBytes(1);
      }
      return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
      int result = in.read(b, off, len);
      if (result != -1) {
        inputStats.incrementProcessedBytes(result);
      }
      return result;
    }

    @Override
    public long skip(long n) throws IOException
    {
      long result = in.skip(n);
      inputStats.incrementProcessedBytes(result);
      return result;
    }
  }
}
