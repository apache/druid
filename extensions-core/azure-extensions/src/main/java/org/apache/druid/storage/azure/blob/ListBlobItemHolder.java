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

package org.apache.druid.storage.azure.blob;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Wrapper class for {@link ListBlobItem} interface, which was missing some useful
 * functionality for telling whether the blob was a cloudBlob or not. This class was
 * added mainly to make testing easier.
 */
public class ListBlobItemHolder
{
  private final ListBlobItem delegate;

  @AssistedInject
  public ListBlobItemHolder(@Assisted ListBlobItem delegate)
  {
    this.delegate = delegate;
  }

  public String getContainerName() throws URISyntaxException, StorageException
  {
    return delegate.getContainer().getName();
  }

  public URI getUri()
  {
    return delegate.getUri();
  }

  public CloudBlobHolder getCloudBlob()
  {
    return new CloudBlobHolder((CloudBlob) delegate);
  }

  public boolean isCloudBlob()
  {
    return delegate instanceof CloudBlob;
  }

  @Override
  public String toString()
  {
    return delegate.toString();
  }
}
