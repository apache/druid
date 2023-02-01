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
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import org.apache.druid.data.input.impl.RetryingInputStream;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.StringUtils;
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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class S3StorageConnector implements StorageConnector
{

  private final S3OutputConfig config;
  private final ServerSideEncryptingAmazonS3 s3Client;

  private static final String DELIM = "/";
  private static final Joiner JOINER = Joiner.on(DELIM).skipNulls();
  private static final long DOWNLOAD_MAX_CHUNK_SIZE = 100_000_000;

  public S3StorageConnector(S3OutputConfig config, ServerSideEncryptingAmazonS3 serverSideEncryptingAmazonS3)
  {
    this.config = config;
    this.s3Client = serverSideEncryptingAmazonS3;
    if (config.getTempDir() != null) {
      try {
        FileUtils.mkdirp(config.getTempDir());
      }
      catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  @Override
  public boolean pathExists(String path)
  {
    return s3Client.doesObjectExist(config.getBucket(), objectPath(path));
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
      readEnd = this.s3Client.getObjectMetadata(config.getBucket(), objectPath(path)).getInstanceLength();
    }

    // build a sequence input stream from chunks
    return new SequenceInputStream(new Enumeration<InputStream>()
    {
      @Override
      public boolean hasMoreElements()
      {
        // don't stop until the whole object is downloaded
        return currReadStart.get() < readEnd;
      }

      @Override
      public InputStream nextElement()
      {
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
                      return s3Client.getObject(object).getObjectContent();
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
          throw new UncheckedIOException(e);
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
              try {
                super.close();
              }
              finally {
                // since endPoint is inclusive in s3's get request API, the next currReadStart is endpoint + 1
                currReadStart.set(endPoint + 1);
                outFile.delete();
              }
            }
          };
        }
        catch (FileNotFoundException e) {
          throw new UncheckedIOException(e);
        }
      }
    });
  }

  @Override
  public OutputStream write(String path) throws IOException
  {
    return new RetryableS3OutputStream(config, s3Client, objectPath(path));
  }

  @Override
  public void deleteFile(String path)
  {
    s3Client.deleteObject(config.getBucket(), objectPath(path));
  }

  @Override
  public void deleteRecursively(String dirName)
  {
    ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
        .withBucketName(config.getBucket())
        .withPrefix(objectPath(dirName));
    ListObjectsV2Result objectListing = s3Client.listObjectsV2(listObjectsRequest);

    while (objectListing.getObjectSummaries().size() > 0) {
      List<DeleteObjectsRequest.KeyVersion> deleteObjectsRequestKeys = objectListing.getObjectSummaries()
                                                                                    .stream()
                                                                                    .map(S3ObjectSummary::getKey)
                                                                                    .map(DeleteObjectsRequest.KeyVersion::new)
                                                                                    .collect(Collectors.toList());
      DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(config.getBucket()).withKeys(
          deleteObjectsRequestKeys);
      s3Client.deleteObjects(deleteObjectsRequest);

      // If the listing is truncated, all S3 objects have been deleted, otherwise, fetch more using the continuation token
      if (objectListing.isTruncated()) {
        listObjectsRequest.withContinuationToken(objectListing.getContinuationToken());
        objectListing = s3Client.listObjectsV2(listObjectsRequest);
      } else {
        break;
      }
    }
  }

  @Override
  public List<String> listDir(String dirName)
  {
    ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
        .withBucketName(config.getBucket())
        .withPrefix(objectPath(dirName))
        .withDelimiter(DELIM);

    List<String> lsResult = new ArrayList<>();

    ListObjectsV2Result objectListing = s3Client.listObjectsV2(listObjectsRequest);

    while (objectListing.getObjectSummaries().size() > 0) {
      objectListing.getObjectSummaries()
                   .stream().map(S3ObjectSummary::getKey)
                   .map(
                       key -> {
                         int index = key.lastIndexOf(DELIM);
                         return key.substring(index + 1);
                       }
                   )
                   .filter(keyPart -> !keyPart.isEmpty())
                   .forEach(lsResult::add);

      if (objectListing.isTruncated()) {
        listObjectsRequest.withContinuationToken(objectListing.getContinuationToken());
        objectListing = s3Client.listObjectsV2(listObjectsRequest);
      } else {
        break;
      }
    }
    return lsResult;
  }

  @Nonnull
  private String objectPath(String path)
  {
    return JOINER.join(config.getPrefix(), path);
  }
}
