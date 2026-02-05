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

package org.apache.druid.testing.embedded.azure;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import org.apache.druid.storage.azure.AzureStorageDruidModule;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Configures the embedded cluster to use Azure blob storage for deep storage of
 * segments and task logs.
 */
public class AzureStorageResource extends TestcontainerResource<AzuriteContainer>
{
  private static final String IMAGE_NAME = "mcr.microsoft.com/azure-storage/azurite:3.35.0";

  /**
   * Default account name used by the {@link AzuriteContainer}.
   */
  public static final String WELL_KNOWN_ACCOUNT_NAME = "devstoreaccount1";

  /**
   * Default account key used by the {@link AzuriteContainer}.
   */
  private static final String WELL_KNOWN_ACCOUNT_KEY =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

  private static final String CONTAINER = "druid-deep-storage";
  private static final String PATH_PREFIX = "druid/segments";

  private CloudBlobClient storageClient;

  @Override
  protected AzuriteContainer createContainer()
  {
    return new AzuriteContainer(DockerImageName.parse(IMAGE_NAME))
        .withCommand("azurite-blob", "--blobHost", "0.0.0.0", "--loose");
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    storageClient = createStorageClient();
    cluster.addExtension(AzureStorageDruidModule.class);

    // Configure storage bucket and base key
    cluster.addCommonProperty("druid.storage.type", "azure");
    cluster.addCommonProperty("druid.azure.container", getAzureContainerName());
    cluster.addCommonProperty("druid.azure.prefix", getPathPrefix());

    // Configure indexer logs
    cluster.addCommonProperty("druid.indexer.logs.type", "azure");
    cluster.addCommonProperty("druid.indexer.logs.container", getAzureContainerName());
    cluster.addCommonProperty("druid.indexer.logs.prefix", "druid/indexing-logs");

    // Configure azure properties
    cluster.addCommonProperty("druid.azure.account", WELL_KNOWN_ACCOUNT_NAME);
    cluster.addCommonProperty("druid.azure.key", WELL_KNOWN_ACCOUNT_KEY);
    cluster.addCommonProperty("druid.azure.protocol", "azurite");
    cluster.addCommonProperty(
        "druid.azure.storageAccountEndpointSuffix",
        "localhost:" + getMappedPort(10000)
    );
  }

  public String getAzureContainerName()
  {
    return CONTAINER;
  }

  public String getPathPrefix()
  {
    return PATH_PREFIX;
  }

  public CloudBlobClient getStorageClient()
  {
    ensureRunning();
    return storageClient;
  }

  private CloudBlobClient createStorageClient()
  {
    try {
      CloudStorageAccount storageAccount = CloudStorageAccount.parse(
          getContainer().getConnectionString()
      );
      return storageAccount.createCloudBlobClient();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
