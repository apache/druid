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

package org.apache.druid.firehose;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.SQLFirehoseDatabaseConnector;
import org.skife.jdbi.v2.DBI;


@JsonTypeName("postgresql")
public class PostgresqlFirehoseDatabaseConnector extends SQLFirehoseDatabaseConnector
{
  private final DBI dbi;
  private final MetadataStorageConnectorConfig connectorConfig;

  public PostgresqlFirehoseDatabaseConnector(
      @JsonProperty("connectorConfig") MetadataStorageConnectorConfig connectorConfig
  )
  {
    this.connectorConfig = connectorConfig;
    final BasicDataSource datasource = getDatasource(connectorConfig);
    datasource.setDriverClassLoader(getClass().getClassLoader());
    datasource.setDriverClassName("org.postgresql.Driver");
    this.dbi = new DBI(datasource);
  }

  @JsonProperty
  public MetadataStorageConnectorConfig getConnectorConfig()
  {
    return connectorConfig;
  }

  @Override
  public DBI getDBI()
  {
    return dbi;
  }
}
