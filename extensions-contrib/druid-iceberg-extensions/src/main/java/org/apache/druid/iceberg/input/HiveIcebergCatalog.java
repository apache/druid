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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.iceberg.guice.HiveConf;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.DynamicConfigProviderUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.hive.HiveCatalog;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

/**
 * Hive Metastore specific implementation of iceberg catalog.
 * Kerberos authentication is performed if the credentials are provided in the catalog properties
 */
public class HiveIcebergCatalog extends IcebergCatalog
{
  public static final String DRUID_DYNAMIC_CONFIG_PROVIDER_KEY = "druid.dynamic.config.provider";
  public static final String TYPE_KEY = "hive";

  @JsonProperty
  private String warehousePath;

  @JsonProperty
  private String catalogUri;

  @JsonProperty
  private Map<String, String> catalogProperties;

  private final Configuration configuration;

  private BaseMetastoreCatalog hiveCatalog;

  private static final Logger log = new Logger(HiveIcebergCatalog.class);

  @JsonCreator
  public HiveIcebergCatalog(
      @JsonProperty("warehousePath") String warehousePath,
      @JsonProperty("catalogUri") String catalogUri,
      @JsonProperty("catalogProperties") @Nullable
          Map<String, Object> catalogProperties,
      @JacksonInject @Json ObjectMapper mapper,
      @JacksonInject @HiveConf Configuration configuration
  )
  {
    this.warehousePath = Preconditions.checkNotNull(warehousePath, "warehousePath cannot be null");
    this.catalogUri = Preconditions.checkNotNull(catalogUri, "catalogUri cannot be null");
    this.catalogProperties = DynamicConfigProviderUtils.extraConfigAndSetStringMap(catalogProperties, DRUID_DYNAMIC_CONFIG_PROVIDER_KEY, mapper);
    this.configuration = configuration;
    this.catalogProperties
        .forEach(this.configuration::set);
    this.hiveCatalog = retrieveCatalog();
  }

  @Override
  public BaseMetastoreCatalog retrieveCatalog()
  {
    if (hiveCatalog == null) {
      hiveCatalog = setupCatalog();
    }
    return hiveCatalog;
  }

  private HiveCatalog setupCatalog()
  {
    HiveCatalog catalog = new HiveCatalog();
    authenticate();
    catalog.setConf(configuration);
    catalogProperties.put("warehouse", warehousePath);
    catalogProperties.put("uri", catalogUri);
    catalog.initialize("hive", catalogProperties);
    return catalog;
  }

  private void authenticate()
  {
    String principal = catalogProperties.getOrDefault("principal", null);
    String keytab = catalogProperties.getOrDefault("keytab", null);
    if (!Strings.isNullOrEmpty(principal) && !Strings.isNullOrEmpty(keytab)) {
      UserGroupInformation.setConfiguration(configuration);
      if (UserGroupInformation.isSecurityEnabled()) {
        try {
          if (UserGroupInformation.getCurrentUser().hasKerberosCredentials() == false
              || !UserGroupInformation.getCurrentUser().getUserName().equals(principal)) {
            log.info("Hive trying to authenticate user [%s] with keytab [%s]..", principal, keytab);
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
          }
        }
        catch (IOException e) {
          throw new ISE(e, "Failed to authenticate user principal [%s] with keytab [%s]", principal, keytab);
        }
      }
    }
  }

  public String getWarehousePath()
  {
    return warehousePath;
  }

  public String getCatalogUri()
  {
    return catalogUri;
  }

  public Map<String, String> getCatalogProperties()
  {
    return catalogProperties;
  }
}
