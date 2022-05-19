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

package org.apache.druid.data.input.azure;

import com.google.common.base.Predicate;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.apache.druid.data.input.RetryingInputEntity;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.storage.azure.AzureByteSource;
import org.apache.druid.storage.azure.AzureByteSourceFactory;
import org.apache.druid.storage.azure.AzureUtils;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Represents an azure based input resource and knows how to read bytes from the given resource.
 */
public class AzureEntity extends RetryingInputEntity
{
  private final CloudObjectLocation location;
  private final AzureByteSource byteSource;

  @AssistedInject
  AzureEntity(
      @Nonnull @Assisted CloudObjectLocation location,
      @Nonnull AzureByteSourceFactory byteSourceFactory
  )
  {
    this.location = location;
    this.byteSource = byteSourceFactory.create(location.getBucket(), location.getPath());
  }

  @Override
  public URI getUri()
  {
    return location.toUri(AzureInputSource.SCHEME);
  }

  @Override
  public Predicate<Throwable> getRetryCondition()
  {
    return AzureUtils.AZURE_RETRY;
  }

  @Override
  protected InputStream readFrom(long offset) throws IOException
  {
    // Get data of the given object and open an input stream
    return byteSource.openStream(offset);
  }

  @Override
  protected String getPath()
  {
    return location.getPath();
  }
}
