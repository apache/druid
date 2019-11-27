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

import org.apache.druid.data.input.impl.RetryingInputStream;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.utils.CompressionUtils;

import java.io.IOException;
import java.io.InputStream;

public abstract class RetryingInputEntity implements InputEntity
{
  /**
   * Open a {@link RetryingInputStream} wrapper for an underlying input stream, optionally decompressing the retrying
   * stream if the file extension matches a known compression, otherwise passing through the retrying stream directly.
   */
  @Override
  public InputStream open() throws IOException
  {
    RetryingInputStream<?> retryingInputStream = new RetryingInputStream<>(
        this,
        new RetryingInputEntityOpenFunction(),
        getRetryCondition(),
        RetryUtils.DEFAULT_MAX_TRIES
    );
    return CompressionUtils.decompress(retryingInputStream, getPath());
  }

  /**
   * Directly opens an {@link InputStream} on the input entity. Decompression should be handled externally, and is
   * handled by the default implementation of {@link #open}, so this should return the raw stream for the object.
   */
  protected InputStream readFromStart() throws IOException
  {
    return readFrom(0);
  }

  /**
   * Directly opens an {@link InputStream} starting at the given offset on the input entity. Decompression should be
   * handled externally, and is handled by the default implementation of {@link #open},this should return the raw stream
   * for the object.
   *
   * @param offset an offset to start reading from. A non-negative integer counting
   *               the number of bytes from the beginning of the entity
   */
  protected abstract InputStream readFrom(long offset) throws IOException;

  /**
   * Get path name for this entity, used by the default implementation of {@link #open} to determine if the underlying
   * stream needs decompressed, based on file extension of the path
   */
  protected abstract String getPath();

  private static class RetryingInputEntityOpenFunction implements ObjectOpenFunction<RetryingInputEntity>
  {
    @Override
    public InputStream open(RetryingInputEntity object) throws IOException
    {
      return object.readFromStart();
    }

    @Override
    public InputStream open(RetryingInputEntity object, long start) throws IOException
    {
      return object.readFrom(start);
    }
  }
}
