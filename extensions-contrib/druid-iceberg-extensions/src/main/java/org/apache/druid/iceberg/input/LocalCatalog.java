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

package org.apache.druid.iceberg.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.hadoop.HadoopCatalog;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * Iceberg catalog implementation that handles file based catalogs created in the local filesystem.
 */
public class LocalCatalog extends IcebergCatalog
{
  public static final String TYPE_KEY = "local";

  @JsonProperty
  private final String warehousePath;

  @JsonProperty
  private final Map<String, String> catalogProperties;

  private BaseMetastoreCatalog catalog;

  @JsonCreator
  public LocalCatalog(
      @JsonProperty("warehousePath") String warehousePath,
      @JsonProperty("catalogProperties") @Nullable
          Map<String, String> catalogProperties
  )
  {
    Preconditions.checkNotNull(warehousePath, "warehousePath is null");
    this.warehousePath = warehousePath;
    this.catalogProperties = catalogProperties;
    this.catalog = retrieveCatalog();

  }

  @JsonProperty
  public String getWarehousePath()
  {
    return warehousePath;
  }

  @JsonProperty
  public Map<String, String> getCatalogProperties()
  {
    return catalogProperties;
  }

  @Override
  public BaseMetastoreCatalog retrieveCatalog()
  {
    if (catalog == null) {
      catalog = setupCatalog();
    }
    return catalog;
  }

  private HadoopCatalog setupCatalog()
  {
    HadoopCatalog hadoopCatalog = new HadoopCatalog();
    hadoopCatalog.setConf(new Configuration());
    catalogProperties.put("warehouse", warehousePath);
    hadoopCatalog.initialize("hadoop", catalogProperties);
    return hadoopCatalog;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LocalCatalog that = (LocalCatalog) o;
    return warehousePath.equals(that.warehousePath)
           && Objects.equals(catalogProperties, that.catalogProperties);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(warehousePath, catalogProperties);
  }
}
