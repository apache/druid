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
import org.apache.druid.data.input.RetryingInputEntity;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.storage.s3.S3StorageDruidModule;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class S3Entity extends RetryingInputEntity
{
  private final ServerSideEncryptingAmazonS3 s3Client;
  private final CloudObjectLocation object;

  S3Entity(ServerSideEncryptingAmazonS3 s3Client, CloudObjectLocation coords)
  {
    this.s3Client = s3Client;
    this.object = coords;
  }

  @Override
  public URI getUri()
  {
    return object.toUri(S3StorageDruidModule.SCHEME);
  }

  @Override
  protected InputStream readFrom(long offset) throws IOException
  {
    final GetObjectRequest request = new GetObjectRequest(object.getBucket(), object.getPath());
    request.setRange(offset);
    try {
      final S3Object s3Object = s3Client.getObject(request);
      if (s3Object == null) {
        throw new ISE(
            "Failed to get an s3 object for bucket[%s], key[%s], and start[%d]",
            object.getBucket(),
            object.getPath(),
            offset
        );
      }
      return s3Object.getObjectContent();
    }
    catch (AmazonS3Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected String getPath()
  {
    return object.getPath();
  }

  @Override
  public Predicate<Throwable> getRetryCondition()
  {
    return S3Utils.S3RETRY;
  }
}
