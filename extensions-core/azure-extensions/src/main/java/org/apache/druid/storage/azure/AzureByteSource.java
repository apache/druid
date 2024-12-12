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

import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.io.ByteSource;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.io.InputStream;

/**
 * Used for getting an {@link InputStream} to an azure resource.
 */
public class AzureByteSource extends ByteSource
{
  private static final Logger log = new Logger(AzureByteSource.class);
  private final AzureStorage azureStorage;
  private final String containerName;
  private final String blobPath;

  @AssistedInject
  public AzureByteSource(
      @Assisted("azureStorage") AzureStorage azureStorage,
      @Assisted("containerName") String containerName,
      @Assisted("blobPath") String blobPath
  )
  {
    this.azureStorage = azureStorage;
    this.containerName = containerName;
    this.blobPath = blobPath;
  }

  @Override
  public InputStream openStream() throws IOException
  {
    return openStream(0L);
  }

  public InputStream openStream(long offset) throws IOException
  {
    try {
      return azureStorage.getBlockBlobInputStream(offset, containerName, blobPath);
    }
    catch (BlobStorageException e) {
      if (AzureUtils.AZURE_RETRY.apply(e)) {
        throw new IOException("Recoverable exception", e);
      }
      log.error("Exception when opening stream to azure resource, containerName: %s, blobPath: %s, Error: %s",
               containerName, blobPath, e.getMessage()
      );
      throw new RuntimeException(e);
    }
  }
}
