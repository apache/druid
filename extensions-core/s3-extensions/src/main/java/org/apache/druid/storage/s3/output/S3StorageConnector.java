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
import org.apache.druid.data.input.impl.RetryingInputStream;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

public class S3StorageConnector implements StorageConnector
{
  private final S3OutputConfig config;
  private final ServerSideEncryptingAmazonS3 s3Client;

  private static final String DELIM = "/";
  private static final Joiner JOINER = Joiner.on(DELIM).skipNulls();

  public S3StorageConnector(S3OutputConfig config, ServerSideEncryptingAmazonS3 serverSideEncryptingAmazonS3)
  {
    this.config = config;
    this.s3Client = serverSideEncryptingAmazonS3;
  }

  @Override
  public boolean pathExists(String path)
  {
    return s3Client.doesObjectExist(config.getBucket(), objectPath(path));
  }

  @Override
  public InputStream read(String path) throws IOException
  {
    return new RetryingInputStream<>(
        new GetObjectRequest(config.getBucket(), objectPath(path)),
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
            final GetObjectRequest offsetObjectRequest = new GetObjectRequest(
                object.getBucketName(),
                object.getKey()
            );
            offsetObjectRequest.setRange(offset);
            return open(offsetObjectRequest);
          }
        },
        S3Utils.S3RETRY,
        config.getMaxRetry()
    );
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

  @Nonnull
  private String objectPath(String path)
  {
    return JOINER.join(config.getPrefix(), path);
  }

}
