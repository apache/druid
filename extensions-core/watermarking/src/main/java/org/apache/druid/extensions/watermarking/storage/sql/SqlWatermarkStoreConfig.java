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

package org.apache.druid.extensions.watermarking.storage.sql;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;

import java.util.Objects;

public class SqlWatermarkStoreConfig extends MetadataStorageConnectorConfig
{
  @JsonProperty
  private boolean createTimelineTables = false;

  public boolean isCreateTimelineTables()
  {
    return createTimelineTables || super.isCreateTables();
  }

  @Override
  public boolean isCreateTables()
  {
    return false;
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
    if (!super.equals(o)) {
      return false;
    }
    SqlWatermarkStoreConfig that = (SqlWatermarkStoreConfig) o;
    return createTimelineTables == that.createTimelineTables;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), createTimelineTables);
  }

  @Override
  public String toString()
  {
    return "SqlWatermarkStoreConfig{" +
           "createTimelineTables=" + createTimelineTables +
           '}';
  }
}
