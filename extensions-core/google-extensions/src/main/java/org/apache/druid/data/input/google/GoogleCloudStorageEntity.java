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

import com.google.common.base.Predicate;
import org.apache.druid.data.input.RetryingInputEntity;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.google.GoogleByteSource;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.storage.google.GoogleUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class GoogleCloudStorageEntity implements RetryingInputEntity
{
  private final GoogleStorage storage;
  private final URI uri;

  GoogleCloudStorageEntity(GoogleStorage storage, URI uri)
  {
    this.storage = storage;
    this.uri = uri;
  }

  @Nullable
  @Override
  public URI getUri()
  {
    return uri;
  }

  @Override
  public InputStream readFrom(long offset) throws IOException
  {
    // Get data of the given object and open an input stream
    final String bucket = uri.getAuthority();
    final String key = StringUtils.maybeRemoveLeadingSlash(uri.getPath());
    final GoogleByteSource byteSource = new GoogleByteSource(storage, bucket, key);
    return byteSource.openStream(offset);
  }

  @Override
  public String getDecompressionPath()
  {
    return StringUtils.maybeRemoveLeadingSlash(uri.getPath());
  }

  @Override
  public Predicate<Throwable> getRetryCondition()
  {
    return GoogleUtils.GOOGLE_RETRY;
  }
}
