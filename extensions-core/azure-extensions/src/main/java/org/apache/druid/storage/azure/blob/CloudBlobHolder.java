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

import com.azure.storage.blob.models.BlobItem;
import java.util.Date;

/**
 * Wrapper for {@link BlobItem}. Used to make testing easier, since {@link BlobItem}
 * is a final class and so is difficult to mock in unit tests.
 */
public class CloudBlobHolder
{
  private final BlobItem delegate;
  private final String container;
  private final String storageAccount;

  public CloudBlobHolder(BlobItem delegate, String container, String storageAccount)
  {
    this.delegate = delegate;
    this.container = container;
    this.storageAccount = storageAccount;
  }

  public String getContainerName()
  {
    return container;
  }

  public String getStorageAccount()
  {
    return storageAccount;
  }

  public String getName()
  {
    return delegate.getName();
  }

  public long getBlobLength()
  {
    return delegate.getProperties().getContentLength();
  }

  public Date getLastModifed()
  {
    return Date.from(delegate.getProperties().getLastModified().toInstant());
  }
}
