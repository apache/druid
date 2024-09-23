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
import org.apache.druid.error.InvalidInput;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.iceberg.guice.HiveConf;
import org.apache.druid.utils.DynamicConfigProviderUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Catalog implementation for Iceberg REST catalogs.
 */
public class RestIcebergCatalog extends IcebergCatalog
{
  public static final String TYPE_KEY = "rest";

  @JsonProperty
  private final String catalogUri;

  @JsonProperty
  private final Map<String, String> catalogProperties;

  private final Configuration configuration;

  private Catalog restCatalog;

  @JsonCreator
  public RestIcebergCatalog(
      @JsonProperty("catalogUri") String catalogUri,
      @JsonProperty("catalogProperties") @Nullable
          Map<String, Object> catalogProperties,
      @JacksonInject @Json ObjectMapper mapper,
      @JacksonInject @HiveConf Configuration configuration
  )
  {
    if (catalogUri == null) {
      throw InvalidInput.exception("catalogUri cannot be null");
    }
    this.catalogUri = catalogUri;
    this.catalogProperties = DynamicConfigProviderUtils.extraConfigAndSetStringMap(
        catalogProperties,
        DRUID_DYNAMIC_CONFIG_PROVIDER_KEY,
        mapper
    );
    this.configuration = configuration;
  }

  @Override
  public Catalog retrieveCatalog()
  {
    if (restCatalog == null) {
      restCatalog = setupCatalog();
    }
    return restCatalog;
  }

  public String getCatalogUri()
  {
    return catalogUri;
  }

  public Map<String, String> getCatalogProperties()
  {
    return catalogProperties;
  }

  private RESTCatalog setupCatalog()
  {
    RESTCatalog restCatalog = new RESTCatalog(
        SessionCatalog.SessionContext.createEmpty(),
        config -> HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build()
    );
    restCatalog.setConf(configuration);
    catalogProperties.put(CatalogProperties.URI, catalogUri);
    restCatalog.initialize("rest", catalogProperties);
    return restCatalog;
  }
}
