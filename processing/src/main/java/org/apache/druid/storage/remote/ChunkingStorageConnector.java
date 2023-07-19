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

package org.apache.druid.storage.remote;

import com.google.common.base.Predicates;
import org.apache.commons.io.input.NullInputStream;
import org.apache.druid.data.input.impl.RetryingInputStream;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.StorageConnector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Enumeration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ChunkingStorageConnector<T> implements StorageConnector
{
  private static final long DOWNLOAD_MAX_CHUNK_SIZE = 100_000_000;

  public ChunkingStorageConnector()
  {

  }

  @Override
  public InputStream read(String path) throws IOException
  {
    return buildInputStream(buildInputParams(path));
  }

  @Override
  public InputStream readRange(String path, long from, long size) throws IOException
  {
    return buildInputStream(buildInputParams(path, from, size));
  }

  public abstract ChunkingStorageConnectorParameters<T> buildInputParams(String path) throws IOException;

  public abstract ChunkingStorageConnectorParameters<T> buildInputParams(String path, long from, long size);

  private InputStream buildInputStream(ChunkingStorageConnectorParameters<T> params)
  {
    AtomicLong currentReadStartPosition = new AtomicLong(params.getStart());
    long readEnd = params.getEnd();

    AtomicBoolean isSequenceStreamClosed = new AtomicBoolean(false);

    return new SequenceInputStream(

        new Enumeration<InputStream>()
        {
          boolean initStream = false;

          @Override
          public boolean hasMoreElements()
          {
            // Checking if the stream was already closed. If it was, then don't iterate over the remaining chunks
            // SequenceInputStream's close method closes all the chunk streams in its close. Since we're opening them
            // lazily, we don't need to close them.
            if (isSequenceStreamClosed.get()) {
              return false;
            }
            // Don't stop until the whole object is downloaded
            return currentReadStartPosition.get() < readEnd;
          }

          @Override
          public InputStream nextElement()
          {
            if (!initStream) {
              initStream = true;
              return new NullInputStream();
            }

            File outFile = new File(
                params.getTempDirSupplier().get().getAbsolutePath(),
                UUID.randomUUID().toString()
            );

            // exclusive
            long currentReadEndPosition = Math.min(
                currentReadStartPosition.get() + DOWNLOAD_MAX_CHUNK_SIZE,
                readEnd
            );

            try {
              if (!outFile.createNewFile()) {
                throw new IOE(
                    StringUtils.format(
                        "Could not create temporary file [%s] for copying [%s]",
                        outFile.getAbsolutePath(),
                        params.getCloudStoragePath()
                    )
                );
              }

              FileUtils.copyLarge(
                  () -> new RetryingInputStream<>(
                      params.getObjectSupplier().getObject(currentReadStartPosition.get(), currentReadEndPosition),
                      params.getObjectOpenFunction(),
                      params.getRetryCondition(),
                      params.getMaxRetry()
                  ),
                  outFile,
                  new byte[8 * 1024],
                  Predicates.alwaysFalse(),
                  1,
                  StringUtils.format(
                      "Retrying copying of [%s] to [%s]",
                      params.getCloudStoragePath(),
                      outFile.getAbsolutePath()
                  )
              );
            }
            catch (IOException e) {
              throw new RE(e, StringUtils.format("Unable to copy [%s] to [%s]", params.getCloudStoragePath(), outFile));
            }

            try {
              AtomicBoolean fileInputStreamClosed = new AtomicBoolean(false);
              return new FileInputStream(outFile)
              {
                @Override
                public void close() throws IOException
                {
                  // close should be idempotent
                  if (fileInputStreamClosed.get()) {
                    return;
                  }
                  fileInputStreamClosed.set(true);
                  super.close();
                  // since endPoint is inclusive in s3's get request API, the next currReadStart is endpoint + 1
                  currentReadStartPosition.set(currentReadEndPosition);
                  if (!outFile.delete()) {
                    throw new RE("Cannot delete temp file [%s]", outFile);
                  }
                }

              };
            }
            catch (FileNotFoundException e) {
              throw new RE(e, StringUtils.format("Unable to find temp file [%s]", outFile));
            }
          }
        }
    )
    {
      @Override
      public void close() throws IOException
      {
        isSequenceStreamClosed.set(true);
        super.close();
      }
    };
  }

  public interface GetObjectFromRangeFunction<T>
  {
    T getObject(long start, long end);
  }
}
