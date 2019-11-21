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

package org.apache.druid.data.input.s3;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.base.Predicate;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;
import org.apache.druid.data.input.impl.prefetch.RetryingInputStream;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.storage.s3.S3Coords;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.apache.druid.utils.CompressionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class S3Entity implements InputEntity
{
  private final S3Coords entityLocation;
  private final ObjectOpenFunction<S3Coords> s3OpenFunction;

  S3Entity(ServerSideEncryptingAmazonS3 s3Client, S3Coords coords)
  {
    this.entityLocation = coords;
    this.s3OpenFunction = new S3OpenFunction(s3Client);
  }

  @Override
  public URI getUri()
  {
    return null;
  }

  @Override
  public InputStream open() throws IOException
  {
    RetryingInputStream<S3Coords> retryingStream = new RetryingInputStream<>(
        entityLocation,
        s3OpenFunction,
        S3Utils.S3RETRY,
        S3Utils.MAX_S3_RETRIES
    );
    return CompressionUtils.decompress(retryingStream, entityLocation.getPath());
  }

  @Override
  public Predicate<Throwable> getFetchRetryCondition()
  {
    return S3Utils.S3RETRY;
  }

  private static class S3OpenFunction implements ObjectOpenFunction<S3Coords>
  {
    private final ServerSideEncryptingAmazonS3 s3Client;

    S3OpenFunction(ServerSideEncryptingAmazonS3 s3Client)
    {
      this.s3Client = s3Client;
    }

    @Override
    public InputStream open(S3Coords object) throws IOException
    {
      return open(object, 0L);
    }

    @Override
    public InputStream open(S3Coords object, long start) throws IOException
    {
      final GetObjectRequest request = new GetObjectRequest(object.getBucket(), object.getPath());
      request.setRange(start);
      try {
        final S3Object s3Object = s3Client.getObject(request);
        if (s3Object == null) {
          throw new ISE(
              "Failed to get an s3 object for bucket[%s], key[%s], and start[%d]",
              object.getBucket(),
              object.getPath(),
              start
          );
        }
        return s3Object.getObjectContent();
      }
      catch (AmazonS3Exception e) {
        throw new IOException(e);
      }
    }
  }
}
