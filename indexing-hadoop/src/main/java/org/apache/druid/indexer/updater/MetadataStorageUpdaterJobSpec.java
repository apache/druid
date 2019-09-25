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

package org.apache.druid.indexer.updater;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.PasswordProvider;

import javax.validation.constraints.NotNull;

/**
 */
public class MetadataStorageUpdaterJobSpec implements Supplier<MetadataStorageConnectorConfig>
{
  @JsonProperty("type")
  @NotNull
  public String type;

  @JsonProperty("connectURI")
  public String connectURI;

  @JsonProperty("user")
  public String user;

  @JsonProperty("password")
  private PasswordProvider passwordProvider;

  @JsonProperty("segmentTable")
  public String segmentTable;

  public String getSegmentTable()
  {
    return segmentTable;
  }

  public String getType()
  {
    return type;
  }

  @Override
  public MetadataStorageConnectorConfig get()
  {
    return new MetadataStorageConnectorConfig()
    {
      @Override
      public String getConnectURI()
      {
        return connectURI;
      }

      @Override
      public String getUser()
      {
        return user;
      }

      @Override
      public String getPassword()
      {
        return passwordProvider == null ? null : passwordProvider.getPassword();
      }
    };
  }

  //Note: Currently it only supports configured segmentTable, other tables should be added if needed
  //by the code using this
  public MetadataStorageTablesConfig getMetadataStorageTablesConfig()
  {
    return new MetadataStorageTablesConfig(
        null,
        null,
        null,
        segmentTable,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }
}
