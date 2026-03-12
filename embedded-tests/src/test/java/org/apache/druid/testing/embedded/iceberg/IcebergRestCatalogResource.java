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
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import javax.annotation.Nullable;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Testcontainer resource wrapping {@code tabulario/iceberg-rest} with a local
 * filesystem warehouse shared via bind mount. The warehouse directory is
 * obtained from the cluster's {@link org.apache.druid.testing.embedded.TestFolder}.
 */
public class IcebergRestCatalogResource extends TestcontainerResource<GenericContainer<?>>
{
  // 1.6.0 is the latest available tag; the REST protocol is compatible with the 1.6.1 client libraries
  private static final String ICEBERG_REST_IMAGE = "tabulario/iceberg-rest:1.6.0";
  private static final int REST_CATALOG_PORT = 8181;
  private static final String CONTAINER_WAREHOUSE_PATH = "/tmp/iceberg-warehouse";

  @Nullable
  private File warehouseDir;

  @Nullable
  private RESTCatalog clientCatalog;

  @Override
  public void beforeStart(EmbeddedDruidCluster cluster)
  {
    warehouseDir = cluster.getTestFolder().getOrCreateFolder("iceberg-warehouse");
  }

  @Override
  protected GenericContainer<?> createContainer()
  {
    if (warehouseDir == null) {
      throw new IllegalStateException("warehouseDir has not been initialized; beforeStart() must be called first");
    }
    // Run as root so the container can write to the bind-mounted warehouse directory
    // regardless of host directory ownership (the default image user is iceberg:iceberg).
    return new GenericContainer<>(ICEBERG_REST_IMAGE)
        .withCreateContainerCmdModifier(cmd -> cmd.withUser("root"))
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

  @Override
  public void stop()
  {
    if (clientCatalog != null) {
      try {
        clientCatalog.close();
      }
      catch (Exception e) {
        // Best-effort cleanup
      }
      clientCatalog = null;
    }
    super.stop();
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
    if (warehouseDir == null) {
      throw new IllegalStateException("warehouseDir has not been initialized; beforeStart() must be called first");
    }
    return warehouseDir;
  }

  /**
   * Creates a namespace in the REST catalog.
   */
  public void createNamespace(String namespace)
  {
    getClientCatalog().createNamespace(Namespace.of(namespace));
  }

  /**
   * Drops a namespace from the REST catalog. Best-effort; ignores errors.
   */
  public void dropNamespace(String namespace)
  {
    try {
      getClientCatalog().dropNamespace(Namespace.of(namespace));
    }
    catch (Exception e) {
      // Best-effort cleanup
    }
  }

  /**
   * Creates a table in the given namespace and returns it.
   */
  public Table createTable(String namespace, String tableName, Schema schema)
  {
    final TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
    return getClientCatalog().createTable(tableId, schema);
  }

  /**
   * Drops a table from the REST catalog. Best-effort; ignores errors.
   */
  public void dropTable(String namespace, String tableName)
  {
    try {
      getClientCatalog().dropTable(TableIdentifier.of(namespace, tableName));
    }
    catch (Exception e) {
      // Best-effort cleanup
    }
  }

  private RESTCatalog getClientCatalog()
  {
    ensureRunning();
    if (clientCatalog == null) {
      clientCatalog = createClientCatalog();
    }
    return clientCatalog;
  }

  private RESTCatalog createClientCatalog()
  {
    // RESTCatalog.initialize() may mutate the properties map, so a mutable map is required
    final Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, getCatalogUri());
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseDir.getAbsolutePath());
    properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.hadoop.HadoopFileIO");

    final RESTCatalog catalog = new RESTCatalog();
    catalog.setConf(new org.apache.hadoop.conf.Configuration());
    catalog.initialize("test-client", properties);
    return catalog;
  }
}
