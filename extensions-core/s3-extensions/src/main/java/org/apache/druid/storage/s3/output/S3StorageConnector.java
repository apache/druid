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

package org.apache.druid.storage.s3.output;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.apache.commons.io.input.NullInputStream;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.RetryingInputStream;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In this implementation, all remote calls to aws s3 are retried {@link S3OutputConfig#getMaxRetry()} times.
 */
public class S3StorageConnector implements StorageConnector
{
  private static final Logger log = new Logger(S3StorageConnector.class);

  private final S3OutputConfig config;
  private final ServerSideEncryptingAmazonS3 s3Client;

  private static final String DELIM = "/";
  private static final Joiner JOINER = Joiner.on(DELIM).skipNulls();
  private static final long DOWNLOAD_MAX_CHUNK_SIZE = 100_000_000;
  private static final int MAX_NUMBER_OF_LISTINGS = 1000;

  public S3StorageConnector(S3OutputConfig config, ServerSideEncryptingAmazonS3 serverSideEncryptingAmazonS3)
  {
    this.config = config;
    this.s3Client = serverSideEncryptingAmazonS3;
    Preconditions.checkNotNull(config, "config is null");
    Preconditions.checkNotNull(config.getTempDir(), "tempDir is null in s3 config");
    try {
      FileUtils.mkdirp(config.getTempDir());
    }
    catch (IOException e) {
      throw new RE(
          e,
          StringUtils.format("Cannot create tempDir : [%s] for s3 storage connector", config.getTempDir())
      );
    }
  }

  @Override
  public boolean pathExists(String path) throws IOException
  {
    try {
      return S3Utils.retryS3Operation(
          () -> s3Client.doesObjectExist(config.getBucket(), objectPath(path)),
          config.getMaxRetry()
      );
    }
    catch (Exception e) {
      log.error("Error occurred while checking if file [%s] exists. Error: [%s]", path, e.getMessage());
      throw new IOException(e);
    }
  }

  @Override
  public InputStream read(String path)
  {
    return buildInputStream(new GetObjectRequest(config.getBucket(), objectPath(path)), path);
  }

  @Override
  public InputStream readRange(String path, long from, long size)
  {
    if (from < 0 || size < 0) {
      throw new IAE(
          "Invalid arguments for reading %s. from = %d, readSize = %d",
          objectPath(path),
          from,
          size
      );
    }
    return buildInputStream(
        new GetObjectRequest(config.getBucket(), objectPath(path)).withRange(from, from + size - 1),
        path
    );
  }

  private InputStream buildInputStream(GetObjectRequest getObjectRequest, String path)
  {
    // fetch the size of the whole object to make chunks
    long readEnd;
    AtomicLong currReadStart = new AtomicLong(0);
    if (getObjectRequest.getRange() != null) {
      currReadStart.set(getObjectRequest.getRange()[0]);
      readEnd = getObjectRequest.getRange()[1] + 1;
    } else {
      try {
        readEnd = S3Utils.retryS3Operation(
            () -> this.s3Client.getObjectMetadata(config.getBucket(), objectPath(path)).getInstanceLength(),
            config.getMaxRetry()
        );
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    AtomicBoolean isSequenceStreamClosed = new AtomicBoolean(false);

    // build a sequence input stream from chunks
    return new SequenceInputStream(new Enumeration<InputStream>()
    {
      boolean initStream = false;
      @Override
      public boolean hasMoreElements()
      {
        // checking if the stream was already closed. If it was, then don't iterate over the remaining chunks
        // SequenceInputStream's close method closes all the chunk streams in its close. Since we're opening them
        // lazily, we don't need to close them.
        if (isSequenceStreamClosed.get()) {
          return false;
        }
        // don't stop until the whole object is downloaded
        return currReadStart.get() < readEnd;
      }

      @Override
      public InputStream nextElement()
      {
        // since Sequence input stream calls nextElement in the constructor, we start chunking as soon as we call read.
        // to avoid that we pass a nullInputStream for the first iteration.
        if (!initStream) {
          initStream = true;
          return new NullInputStream();
        }
        File outFile = new File(config.getTempDir().getAbsolutePath(), UUID.randomUUID().toString());
        // in a single chunk, only download a maximum of DOWNLOAD_MAX_CHUNK_SIZE
        long endPoint = Math.min(currReadStart.get() + DOWNLOAD_MAX_CHUNK_SIZE, readEnd) - 1;
        try {
          if (!outFile.createNewFile()) {
            throw new IOE(
                StringUtils.format(
                    "Could not create temporary file [%s] for copying [%s]",
                    outFile.getAbsolutePath(),
                    objectPath(path)
                )
            );
          }
          FileUtils.copyLarge(
              () -> new RetryingInputStream<>(
                  new GetObjectRequest(
                      config.getBucket(),
                      objectPath(path)
                  ).withRange(currReadStart.get(), endPoint),
                  new ObjectOpenFunction<GetObjectRequest>()
                  {
                    @Override
                    public InputStream open(GetObjectRequest object)
                    {
                      try {
                        return S3Utils.retryS3Operation(
                            () -> s3Client.getObject(object).getObjectContent(),
                            config.getMaxRetry()
                        );
                      }
                      catch (Exception e) {
                        throw new RuntimeException(e);
                      }
                    }

                    @Override
                    public InputStream open(GetObjectRequest object, long offset)
                    {
                      if (object.getRange() != null) {
                        long[] oldRange = object.getRange();
                        object.setRange(oldRange[0] + offset, oldRange[1]);
                      } else {
                        object.setRange(offset);
                      }
                      return open(object);
                    }
                  },
                  S3Utils.S3RETRY,
                  config.getMaxRetry()
              ),
              outFile,
              new byte[8 * 1024],
              Predicates.alwaysFalse(),
              1,
              StringUtils.format("Retrying copying of [%s] to [%s]", objectPath(path), outFile.getAbsolutePath())
          );
        }
        catch (IOException e) {
          throw new RE(e, StringUtils.format("Unable to copy [%s] to [%s]", objectPath(path), outFile));
        }
        try {
          AtomicBoolean isClosed = new AtomicBoolean(false);
          return new FileInputStream(outFile)
          {
            @Override
            public void close() throws IOException
            {
              // close should be idempotent
              if (isClosed.get()) {
                return;
              }
              isClosed.set(true);
              super.close();
              // since endPoint is inclusive in s3's get request API, the next currReadStart is endpoint + 1
              currReadStart.set(endPoint + 1);
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
    })
    {
      @Override
      public void close() throws IOException
      {
        isSequenceStreamClosed.set(true);
        super.close();
      }
    };
  }

  @Override
  public OutputStream write(String path) throws IOException
  {
    return new RetryableS3OutputStream(config, s3Client, objectPath(path));
  }

  @Override
  public void deleteFile(String path) throws IOException
  {
    try {
      S3Utils.retryS3Operation(() -> {
        s3Client.deleteObject(config.getBucket(), objectPath(path));
        return null;
      }, config.getMaxRetry());
    }
    catch (Exception e) {
      log.error("Error occurred while deleting file at path [%s]. Error: [%s]", path, e.getMessage());
      throw new IOException(e);
    }
  }

  @Override
  public void deleteFiles(Iterable<String> paths) throws IOException
  {
    int currentItemSize = 0;
    List<DeleteObjectsRequest.KeyVersion> versions = new ArrayList<>();

    for (String path : paths) {
      // appending base path to each path
      versions.add(new DeleteObjectsRequest.KeyVersion(objectPath(path)));
      currentItemSize++;
      if (currentItemSize == MAX_NUMBER_OF_LISTINGS) {
        deleteKeys(versions);
        // resetting trackers
        versions.clear();
        currentItemSize = 0;
      }
    }
    // deleting remaining elements
    if (currentItemSize != 0) {
      deleteKeys(versions);
    }
  }

  private void deleteKeys(List<DeleteObjectsRequest.KeyVersion> versions) throws IOException
  {
    try {
      S3Utils.deleteBucketKeys(s3Client, config.getBucket(), versions, config.getMaxRetry());
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }


  @Override
  public void deleteRecursively(String dirName) throws IOException
  {
    try {
      S3Utils.deleteObjectsInPath(
          s3Client,
          MAX_NUMBER_OF_LISTINGS,
          config.getBucket(),
          objectPath(dirName),
          Predicates.alwaysTrue(),
          config.getMaxRetry()
      );
    }
    catch (Exception e) {
      log.error("Error occurred while deleting files in path [%s]. Error: [%s]", dirName, e.getMessage());
      throw new IOException(e);
    }
  }

  @Override
  public Iterator<String> listDir(String dirName) throws IOException
  {
    final String prefixBasePath = objectPath(dirName);
    try {

      Iterator<S3ObjectSummary> files = S3Utils.objectSummaryIterator(
          s3Client,
          ImmutableList.of(new CloudObjectLocation(
              config.getBucket(),
              prefixBasePath
          ).toUri("s3")),
          MAX_NUMBER_OF_LISTINGS,
          config.getMaxRetry()
      );

      return Iterators.transform(files, summary -> {
        String[] size = summary.getKey().split(prefixBasePath, 2);
        if (size.length > 1) {
          return size[1];
        } else {
          return "";
        }
      });
    }
    catch (Exception e) {
      log.error("Error occoured while listing files at path [%s]. Error: [%s]", dirName, e.getMessage());
      throw new IOException(e);
    }
  }

  @Nonnull
  private String objectPath(String path)
  {
    return JOINER.join(config.getPrefix(), path);
  }
}
