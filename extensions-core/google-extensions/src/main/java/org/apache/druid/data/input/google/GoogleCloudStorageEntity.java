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

package org.apache.druid.data.input.google;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;
import org.apache.druid.data.input.impl.prefetch.RetryingInputStream;
import org.apache.druid.storage.google.GoogleByteSource;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.storage.google.GoogleUtils;
import org.apache.druid.utils.CompressionUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class GoogleCloudStorageEntity implements InputEntity
{
  private final GoogleByteSourceOpenFunction googleByteSourceOpenFunction;
  private final GoogleByteSource byteSource;
  private final URI uri;

  GoogleCloudStorageEntity(GoogleStorage storage, URI uri)
  {
    this.googleByteSourceOpenFunction = new GoogleByteSourceOpenFunction();
    this.uri = Preconditions.checkNotNull(uri);
    final String bucket = uri.getAuthority();
    final String key = GoogleUtils.extractGoogleCloudStorageObjectKey(uri);
    this.byteSource = new GoogleByteSource(storage, bucket, key);
  }

  @Nullable
  @Override
  public URI getUri()
  {
    return uri;
  }

  @Override
  public InputStream open() throws IOException
  {
    RetryingInputStream<GoogleByteSource> retryingStream = new RetryingInputStream<>(
        byteSource,
        googleByteSourceOpenFunction,
        GoogleUtils.GOOGLE_RETRY,
        GoogleUtils.MAX_BYTESOURCE_RETRIES
    );
    return CompressionUtils.decompress(retryingStream, uri.getPath());
  }

  @Override
  public Predicate<Throwable> getFetchRetryCondition()
  {
    return GoogleUtils.GOOGLE_RETRY;
  }

  private static class GoogleByteSourceOpenFunction implements ObjectOpenFunction<GoogleByteSource>
  {
    @Override
    public InputStream open(GoogleByteSource object) throws IOException
    {
      return open(object, 0L);
    }

    @Override
    public InputStream open(GoogleByteSource object, long start) throws IOException
    {
      return object.openStream(start);
    }
  }
}
