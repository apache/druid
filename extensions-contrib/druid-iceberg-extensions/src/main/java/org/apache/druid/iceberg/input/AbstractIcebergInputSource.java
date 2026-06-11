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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.common.config.Configs;
import org.apache.druid.iceberg.filter.IcebergFilter;
import org.apache.iceberg.Table;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

public abstract class AbstractIcebergInputSource
{
  protected final String tableName;
  protected final String namespace;
  protected final IcebergCatalog icebergCatalog;
  protected final IcebergFilter icebergFilter;
  protected final DateTime snapshotTime;
  protected final ResidualFilterMode residualFilterMode;

  protected AbstractIcebergInputSource(
      final String tableName,
      final String namespace,
      @Nullable final IcebergFilter icebergFilter,
      final IcebergCatalog icebergCatalog,
      @Nullable final DateTime snapshotTime,
      @Nullable final ResidualFilterMode residualFilterMode
  )
  {
    this.tableName = Preconditions.checkNotNull(tableName, "tableName cannot be null");
    this.namespace = Preconditions.checkNotNull(namespace, "namespace cannot be null");
    this.icebergCatalog = Preconditions.checkNotNull(icebergCatalog, "icebergCatalog cannot be null");
    this.icebergFilter = icebergFilter;
    this.snapshotTime = snapshotTime;
    this.residualFilterMode = Configs.valueOrDefault(residualFilterMode, ResidualFilterMode.IGNORE);
  }

  @JsonProperty
  public String getTableName()
  {
    return tableName;
  }

  @JsonProperty
  public String getNamespace()
  {
    return namespace;
  }

  @JsonProperty
  public IcebergCatalog getIcebergCatalog()
  {
    return icebergCatalog;
  }

  @JsonProperty
  public IcebergFilter getIcebergFilter()
  {
    return icebergFilter;
  }

  @Nullable
  @JsonProperty
  public DateTime getSnapshotTime()
  {
    return snapshotTime;
  }

  @JsonProperty
  public ResidualFilterMode getResidualFilterMode()
  {
    return residualFilterMode;
  }

  protected Table retrieveTable()
  {
    return icebergCatalog.retrieveTable(namespace, tableName);
  }
}
