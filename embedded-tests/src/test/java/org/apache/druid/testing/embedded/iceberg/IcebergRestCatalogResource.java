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

package org.apache.druid.testing.embedded.iceberg;

import org.apache.druid.iceberg.common.IcebergDruidModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;

/**
 * Testcontainer resource wrapping {@code tabulario/iceberg-rest} with a local
 * filesystem warehouse shared via bind mount.
 */
public class IcebergRestCatalogResource extends TestcontainerResource<GenericContainer<?>>
{
  // 1.6.0 is the latest available tag; the REST protocol is compatible with the 1.6.1 client libraries
  private static final String ICEBERG_REST_IMAGE = "tabulario/iceberg-rest:1.6.0";
  private static final int REST_CATALOG_PORT = 8181;
  private static final String CONTAINER_WAREHOUSE_PATH = "/tmp/iceberg-warehouse";

  private final File warehouseDir;

  public IcebergRestCatalogResource(File warehouseDir)
  {
    this.warehouseDir = warehouseDir;
  }

  @Override
  protected GenericContainer<?> createContainer()
  {
    return new GenericContainer<>(ICEBERG_REST_IMAGE)
        .withExposedPorts(REST_CATALOG_PORT)
        .withFileSystemBind(warehouseDir.getAbsolutePath(), CONTAINER_WAREHOUSE_PATH, BindMode.READ_WRITE)
        .withEnv("CATALOG_WAREHOUSE", CONTAINER_WAREHOUSE_PATH)
        .withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.hadoop.HadoopFileIO")
        .waitingFor(Wait.forHttp("/v1/config").forPort(REST_CATALOG_PORT));
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    cluster.addExtension(IcebergDruidModule.class);
  }

  /**
   * Returns the REST catalog URI accessible from the host (via mapped port).
   */
  public String getCatalogUri()
  {
    ensureRunning();
    return StringUtils.format(
        "http://%s:%d",
        getContainer().getHost(),
        getContainer().getMappedPort(REST_CATALOG_PORT)
    );
  }

  /**
   * Returns the host-side warehouse directory.
   */
  public File getWarehouseDir()
  {
    return warehouseDir;
  }
}
