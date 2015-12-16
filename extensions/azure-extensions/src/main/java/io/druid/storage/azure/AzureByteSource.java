/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.storage.azure;

import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.microsoft.azure.storage.StorageException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

public class AzureByteSource extends ByteSource
{

  final private AzureStorage azureStorage;
  final private String containerName;
  final private String blobPath;

  public AzureByteSource(
      AzureStorage azureStorage,
      String containerName,
      String blobPath
  )
  {
    this.azureStorage = azureStorage;
    this.containerName = containerName;
    this.blobPath = blobPath;
  }

  @Override
  public InputStream openStream() throws IOException
  {
    try {
      return azureStorage.getBlobInputStream(containerName, blobPath);
    }
    catch (StorageException | URISyntaxException e) {
      if (AzureUtils.AZURE_RETRY.apply(e)) {
        throw new IOException("Recoverable exception", e);
      }
      throw Throwables.propagate(e);
    }
  }
}
