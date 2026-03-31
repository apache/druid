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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.auth.TaskAuthContext;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.iceberg.guice.HiveConf;
import org.apache.druid.utils.DynamicConfigProviderUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.rest.RESTSessionCatalog;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Catalog implementation for Iceberg REST catalogs.
 *
 * <p>Uses {@link RESTSessionCatalog} so that a per-request {@link SessionCatalog.SessionContext}
 * carrying credentials from {@link TaskAuthContext} is passed to every catalog operation.
 */
public class RestIcebergCatalog extends IcebergCatalog
{
  public static final String TYPE_KEY = "rest";

  @JsonProperty
  private final String catalogUri;

  @JsonProperty
  private final Map<String, String> catalogProperties;

  private final Configuration configuration;

  @JsonIgnore
  private transient TaskAuthContext taskAuthContext;

  private volatile RESTSessionCatalog restSessionCatalog;

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

  /**
   * Returns a Catalog instance that uses the current session context for all operations.
   * The returned Catalog wraps the underlying RESTSessionCatalog and passes session
   * credentials to all catalog operations (listTables, loadTable, etc.).
   */
  @Override
  public Catalog retrieveCatalog()
  {
    return getOrCreateSessionCatalog().asCatalog(buildSessionContext());
  }

  public String getCatalogUri()
  {
    return catalogUri;
  }

  public Map<String, String> getCatalogProperties()
  {
    return catalogProperties;
  }

  @Override
  public void setTaskAuthContext(@Nullable TaskAuthContext taskAuthContext)
  {
    this.taskAuthContext = taskAuthContext;
  }

  private RESTSessionCatalog getOrCreateSessionCatalog()
  {
    RESTSessionCatalog catalog = restSessionCatalog;
    if (catalog == null) {
      synchronized (this) {
        catalog = restSessionCatalog;
        if (catalog == null) {
          catalog = setupCatalog();
          restSessionCatalog = catalog;
        }
      }
    }
    return catalog;
  }

  private RESTSessionCatalog setupCatalog()
  {
    RESTSessionCatalog catalog = new RESTSessionCatalog();
    catalog.setConf(configuration);
    Map<String, String> props = new HashMap<>(catalogProperties);
    props.put(CatalogProperties.URI, catalogUri);
    catalog.initialize("rest", props);
    return catalog;
  }

  /**
   * Builds a session context from TaskAuthContext credentials if available,
   * or an empty context otherwise.
   */
  private SessionCatalog.SessionContext buildSessionContext()
  {
    if (taskAuthContext != null) {
      Map<String, String> credentials = taskAuthContext.getCredentials();
      if (credentials != null && !credentials.isEmpty()) {
        return new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(),
            taskAuthContext.getIdentity(),
            credentials,
            Collections.emptyMap()
        );
      }
    }
    return SessionCatalog.SessionContext.createEmpty();
  }

  @Override
  @Nullable
  protected VendedCredentials extractCredentials(Table table)
  {
    VendedCredentials credentials = VendedCredentials.extractFrom(table.properties());
    if (credentials != null) {
      return credentials;
    }

    // Fall back to FileIO properties; don't let extraction failures block the scan
    try {
      return VendedCredentials.extractFrom(table.io().properties());
    }
    catch (Exception e) {
      return null;
    }
  }
}
