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

package org.apache.druid.metadata;

import javax.annotation.Nullable;

public class TestMetadataStorageConnector implements MetadataStorageConnector
{
  @Override
  public Void insertOrUpdate(String tableName, String keyColumn, String valueColumn, String key, byte[] value)
  {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public byte[] lookup(String tableName, String keyColumn, String valueColumn, String key)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createDataSourceTable()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createPendingSegmentsTable()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createSegmentTable()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createUpgradeSegmentsTable()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createRulesTable()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createConfigTable()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTaskTables()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createAuditTable()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createSupervisorsTable()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteAllRecords(String tableName)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createSegmentSchemasTable()
  {
    throw new UnsupportedOperationException();
  }
}
