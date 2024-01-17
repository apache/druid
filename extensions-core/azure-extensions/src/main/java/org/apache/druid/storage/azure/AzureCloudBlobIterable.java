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

package org.apache.druid.storage.azure;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.apache.druid.storage.azure.blob.CloudBlobHolder;

import java.net.URI;
import java.util.Iterator;

/**
 * {@link Iterable} for {@link CloudBlobHolder} objects.
 */
public class AzureCloudBlobIterable implements Iterable<CloudBlobHolder>
{
  private final Iterable<URI> prefixes;
  private final int maxListingLength;
  private final AzureCloudBlobIteratorFactory azureCloudBlobIteratorFactory;
  private final AzureStorage azureStorage;

  @AssistedInject
  public AzureCloudBlobIterable(
      AzureCloudBlobIteratorFactory azureCloudBlobIteratorFactory,
      @Assisted final Iterable<URI> prefixes,
      @Assisted final int maxListingLength,
      @Assisted final AzureStorage azureStorage
  )
  {
    this.azureCloudBlobIteratorFactory = azureCloudBlobIteratorFactory;
    this.prefixes = prefixes;
    this.maxListingLength = maxListingLength;
    this.azureStorage = azureStorage;
  }

  @Override
  public Iterator<CloudBlobHolder> iterator()
  {
    return azureCloudBlobIteratorFactory.create(prefixes, maxListingLength, azureStorage);
  }
}
