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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.DynamicConfigProviderUtils;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Glue specific implementation of iceberg catalog.
 * It expects Catalog Properties key:value pair, which is iceberg compatible:
 * https://iceberg.apache.org/docs/latest/configuration/#catalog-properties
 */
public class GlueIcebergCatalog extends IcebergCatalog
{
  private static final String CATALOG_NAME = "glue";
  private Catalog catalog;

  public static final String TYPE_KEY = "glue";

  @JsonProperty
  private Map<String, String> catalogProperties;

  @JsonProperty
  private final Boolean caseSensitive;
  private static final Logger log = new Logger(GlueIcebergCatalog.class);

  /**
  * catalogProperties must have all the config that iceberg glue catalog expect.
  * Ref: https://iceberg.apache.org/docs/nightly/kafka-connect/?h=kafka#glue-example
  * and https://iceberg.apache.org/concepts/catalog/?h=catalog
  * e.g.
  * "catalogProperties" :
         {
           "type" : "glue",
           "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
           "warehouse": "s3://bucket/iceberg_catalog/druid/warehouse"
         }
  *
  * */
  @JsonCreator
  public GlueIcebergCatalog(
      @JsonProperty("catalogProperties") @Nullable Map<String, Object> catalogProperties,
      @JsonProperty("caseSensitive") Boolean caseSensitive,
      @JacksonInject @Json ObjectMapper mapper
  )
  {
    this.catalogProperties = DynamicConfigProviderUtils.extraConfigAndSetStringMap(
        catalogProperties,
        DRUID_DYNAMIC_CONFIG_PROVIDER_KEY,
        mapper
    );
    this.caseSensitive = caseSensitive == null ? true : caseSensitive;
    this.catalog = retrieveCatalog();
  }

  @Override
  public Catalog retrieveCatalog()
  {
    if (catalog == null) {
      log.info("catalog is null, setting up default glue catalog.");
      catalog = setupGlueCatalog();
    }
    log.info("Glue catalog set [%s].", catalog.toString());
    return catalog;
  }

  private Catalog setupGlueCatalog()
  {
    // We are not passing any hadoop config, third parameter is null
    catalogProperties.put("type", TYPE_KEY);
    catalog = CatalogUtil.buildIcebergCatalog(CATALOG_NAME, catalogProperties, null);
    return catalog;
  }

  @Override
  public boolean isCaseSensitive()
  {
    return caseSensitive;
  }
}
