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
import org.apache.druid.storage.google.GoogleByteSource;
import org.apache.druid.storage.google.GoogleUtils;
import org.apache.druid.utils.CompressionUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class GoogleCloudStorageEntity implements RetryingInputEntity
{
  private final GoogleByteSource byteSource;

  GoogleCloudStorageEntity(GoogleByteSource byteSource)
  {
    this.byteSource = byteSource;
  }

  @Nullable
  @Override
  public URI getUri()
  {
    return null;
  }

  @Override
  public InputStream readFrom(long offset) throws IOException
  {
    return CompressionUtils.decompress(byteSource.openStream(offset), byteSource.getPath());
  }

  @Override
  public Predicate<Throwable> getRetryCondition()
  {
    return GoogleUtils.GOOGLE_RETRY;
  }
}
