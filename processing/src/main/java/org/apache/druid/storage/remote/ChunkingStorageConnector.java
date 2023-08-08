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

/**
 * An abstract implementation of the storage connectors that download the file from the remote storage in chunks
 * and presents the downloaded chunks as a single {@link InputStream} for the consumers of the connector.
 * This implementation benefits over keeping the InputStream to the remote source open since we don't require the
 * connection to be open for the entire duration.
 * Checkout {@link ChunkingStorageConnectorParameters} to see the inputs required to support chunking
 */
public abstract class ChunkingStorageConnector<T> implements StorageConnector
{
  /**
   * Default size for chunking of the storage connector. Set to 100MBs to keep the chunk size small relative to the
   * total frame size, while also preventing a large number of calls to the remote storage. While fetching a single
   * file, 100MBs would be required in the disk space.
   */
  private static final long DOWNLOAD_MAX_CHUNK_SIZE_BYTES = 100_000_000;

  /**
   * Default fetch buffer size while copying from the remote location to the download file. Set to default sizing given
   * in the {@link org.apache.commons.io.IOUtils}
   */
  private static final int FETCH_BUFFER_SIZE_BYTES = 8 * 1024;

  private final long chunkSizeBytes;

  public ChunkingStorageConnector()
  {
    this(DOWNLOAD_MAX_CHUNK_SIZE_BYTES);
  }

  public ChunkingStorageConnector(
      final long chunkSizeBytes
  )
  {
    this.chunkSizeBytes = chunkSizeBytes;
  }

  @Override
  public InputStream read(String path) throws IOException
  {
    return buildInputStream(buildInputParams(path));
  }

  @Override
  public InputStream readRange(String path, long from, long size)
  {
    return buildInputStream(buildInputParams(path, from, size));
  }

  public abstract ChunkingStorageConnectorParameters<T> buildInputParams(String path) throws IOException;

  public abstract ChunkingStorageConnectorParameters<T> buildInputParams(String path, long from, long size);

  private InputStream buildInputStream(ChunkingStorageConnectorParameters<T> params)
  {
    // Position from where the read needs to be resumed
    final AtomicLong currentReadStartPosition = new AtomicLong(params.getStart());

    // Final position, exclusive
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

            long currentReadEndPosition = Math.min(
                currentReadStartPosition.get() + chunkSizeBytes,
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
                  new byte[FETCH_BUFFER_SIZE_BYTES],
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
