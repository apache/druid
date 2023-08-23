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
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.apache.commons.io.IOUtils;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.remote.ChunkingStorageConnector;
import org.apache.druid.storage.remote.ChunkingStorageConnectorParameters;
import org.apache.druid.storage.s3.NoopServerSideEncryption;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * In this implementation, all remote calls to aws s3 are retried {@link S3OutputConfig#getMaxRetry()} times.
 */
public class S3StorageConnector extends ChunkingStorageConnector<GetObjectRequest>
{
  private static final Logger log = new Logger(S3StorageConnector.class);
  private static final int DOWNLOAD_PARALLELISM = 4;

  private final S3OutputConfig config;
  private final ServerSideEncryptingAmazonS3 s3Client;

  @Nullable
  private final TransferManager transferManager;

  private static final String DELIM = "/";
  private static final Joiner JOINER = Joiner.on(DELIM).skipNulls();
  private static final int MAX_NUMBER_OF_LISTINGS = 1000;
  private static final long DOWNLOAD_SIZE_BYTES = 32 * 1024 * 1024;

  private final long uploadChunkSize;

  // cacheLocally is old behaviour
  private final boolean cacheLocally;

  public S3StorageConnector(S3OutputConfig config, ServerSideEncryptingAmazonS3 serverSideEncryptingAmazonS3)
  {
    this(
        config,
        serverSideEncryptingAmazonS3,
        config.getChunkSize(),
        !(config.isTestingTransferManager())
    );
  }

  private S3StorageConnector(
      S3OutputConfig config,
      ServerSideEncryptingAmazonS3 serverSideEncryptingAmazonS3,
      long uploadChunkSize,
      boolean cacheLocally
  )
  {
    super(
        cacheLocally ? config.getChunkSize() : DOWNLOAD_PARALLELISM * DOWNLOAD_SIZE_BYTES,
        cacheLocally
    );
    this.uploadChunkSize = uploadChunkSize;
    this.cacheLocally = cacheLocally;
    this.config = config;
    this.s3Client = serverSideEncryptingAmazonS3;

    if (!cacheLocally) {
      this.transferManager = TransferManagerBuilder.standard()
                                                   .withS3Client(serverSideEncryptingAmazonS3.getUnderlyingS3Client())
                                                   .build();
    } else {
      this.transferManager = null;
    }

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
  public ChunkingStorageConnectorParameters<GetObjectRequest> buildInputParams(String path)
  {
    long size;
    try {
      size = S3Utils.retryS3Operation(
          () -> this.s3Client.getObjectMetadata(config.getBucket(), objectPath(path)).getInstanceLength(),
          config.getMaxRetry()
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    return buildInputParams(path, 0, size);
  }

  @Override
  public ChunkingStorageConnectorParameters<GetObjectRequest> buildInputParams(String path, long from, long size)
  {
    ChunkingStorageConnectorParameters.Builder<GetObjectRequest> builder = new ChunkingStorageConnectorParameters.Builder<>();
    builder.start(from);
    builder.end(from + size);
    builder.cloudStoragePath(objectPath(path));
    builder.tempDirSupplier(config::getTempDir);
    builder.maxRetry(config.getMaxRetry());
    builder.retryCondition(S3Utils.S3RETRY);
    builder.objectSupplier((start, end) -> new GetObjectRequest(config.getBucket(), objectPath(path)).withRange(
        start,
        end - 1
    ));
    builder.objectOpenFunction(new ObjectOpenFunction<GetObjectRequest>()
    {
      @Override
      public InputStream open(GetObjectRequest object)
      {
        try {
          if (cacheLocally) {
            return S3Utils.retryS3Operation(
                () -> s3Client.getObject(object).getObjectContent(),
                config.getMaxRetry()
            );
          }

          DateTime start = DateTimes.nowUtc();
          InputStream is = implementParallelDownload(object, 4, DOWNLOAD_SIZE_BYTES);

          log.info(
              "Download path [%s], size [%d] took [%d] ms",
              path,
              size,
              new Interval(start, DateTimes.nowUtc()).toDurationMillis()
          );

          return is;
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
    });
    return builder.build();
  }

  @Override
  public OutputStream write(String path) throws IOException
  {
    return new RetryableS3OutputStream(config, s3Client, objectPath(path), uploadChunkSize);
  }

  @Override
  public void deleteFile(String path) throws IOException
  {
    try {
      final String fullPath = objectPath(path);
      log.debug("Deleting file at bucket: [%s], path: [%s]", config.getBucket(), fullPath);

      S3Utils.retryS3Operation(() -> {
        s3Client.deleteObject(config.getBucket(), fullPath);
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
      log.error("Error occurred while deleting files from S3. Error: [%s]", e.getMessage());
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

  // Add some buffering as well
  InputStream implementParallelDownload(GetObjectRequest getObjectRequest, int parallelism, long rangeSize)
  {
    ForkJoinPool fjp = new ForkJoinPool(parallelism);
    long start = getObjectRequest.getRange()[0];
    long end = getObjectRequest.getRange()[1];
    List<Pair<Long, Long>> divisions = new ArrayList<>();

    for (long st = start; st <= end; st += rangeSize) {
      divisions.add(Pair.of(st, Math.min(st + rangeSize - 1, end)));
    }

    try {
      List<Pair<Long, File>> startToIs =
          fjp.submit(() -> divisions.stream()
                                    .parallel()
                                    .map(division -> {
                                      try {
                                        File outFile = new File(
                                            config.getTempDir().getAbsolutePath(),
                                            UUID.randomUUID().toString()
                                        );
                                        DateTime startTime = DateTimes.nowUtc();
                                        FileOutputStream fos = new FileOutputStream(outFile);
                                        IOUtils.copy(
                                            s3Client.getObject(
                                                new GetObjectRequest(
                                                    getObjectRequest.getBucketName(),
                                                    getObjectRequest.getKey()
                                                ).withRange(division.lhs, division.rhs)
                                            ).getObjectContent(),
                                            fos
                                        );
                                        fos.close();
                                        DateTime endTime = DateTimes.nowUtc();
                                        log.info(
                                            "Download path [%s], chunk [%d], started from [%s], ended at [%s], total time [%d]",
                                            getObjectRequest.getKey(),
                                            division.lhs,
                                            startTime.toString(),
                                            endTime,
                                            new Interval(startTime, endTime).toDurationMillis()
                                        );


                                        return Pair.of(
                                            division.lhs,
                                            outFile
                                        );
                                      }
                                      catch (IOException e) {
                                        throw new RE(e);
                                      }
                                    })
                                    .collect(Collectors.toList())
          ).get();
      startToIs.sort((o1, o2) -> {
        if (o1.lhs < o2.lhs) {
          return -1;
        } else if (o1.lhs > o2.lhs) {
          return 1;
        } else {
          return 0;
        }
      });
      return new SequenceInputStream(
          Collections.enumeration(startToIs.stream().map(pair -> {
            try {
              AtomicBoolean fileInputStreamClosed = new AtomicBoolean(false);
              return new FileInputStream(pair.rhs)
              {
                @Override
                public void close() throws IOException
                {
                  if (fileInputStreamClosed.get()) {
                    return;
                  }
                  fileInputStreamClosed.set(true);
                  super.close();
                  if (!pair.rhs.delete()) {
                    throw new RE("Cannot delete temp file [%s]", pair.rhs);
                  }
                }
              };
            }
            catch (FileNotFoundException e) {
              throw new RE(e);
            }
          }).collect(Collectors.toList()))
      );
    }
    catch (InterruptedException e) {
      throw new RE(e);
    }
    catch (ExecutionException e) {
      throw new RE(e);
    }
  }
}
