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

package org.apache.druid.data.input.aliyun;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import com.google.common.base.Predicate;
import org.apache.druid.data.input.RetryingInputEntity;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.storage.aliyun.OssStorageDruidModule;
import org.apache.druid.storage.aliyun.OssUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class OssEntity extends RetryingInputEntity
{
  private final OSS ossClient;
  private final CloudObjectLocation object;

  OssEntity(OSS ossClient, CloudObjectLocation coords)
  {
    this.ossClient = ossClient;
    this.object = coords;
  }

  @Override
  public URI getUri()
  {
    return object.toUri(OssStorageDruidModule.SCHEME);
  }

  @Override
  protected InputStream readFrom(long offset) throws IOException
  {
    final GetObjectRequest request = new GetObjectRequest(object.getBucket(), object.getPath());
    request.setRange(offset, -1 /*from offset to end*/);

    try {
      final OSSObject ossObject = ossClient.getObject(request);
      if (ossObject == null) {
        throw new ISE(
            "Failed to get an Aliyun OSS object for bucket[%s], key[%s], and start[%d]",
            object.getBucket(),
            object.getPath(),
            offset
        );
      }
      return ossObject.getObjectContent();
    }
    catch (OSSException e) {
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
    return OssUtils.RETRYABLE;
  }
}
