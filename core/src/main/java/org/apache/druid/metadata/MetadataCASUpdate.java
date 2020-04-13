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

/**
 * Expresses a single compare-and-swap update for MetadataStorageConnector's compareAndSwap method
 */
public class MetadataCASUpdate
{
  private final String tableName;
  private final String keyColumn;
  private final String valueColumn;
  private final String key;
  private final byte[] oldValue;
  private final byte[] newValue;

  public MetadataCASUpdate(
      String tableName,
      String keyColumn,
      String valueColumn,
      String key,
      byte[] oldValue,
      byte[] newValue
  )
  {
    this.tableName = tableName;
    this.keyColumn = keyColumn;
    this.valueColumn = valueColumn;
    this.key = key;
    this.oldValue = oldValue;
    this.newValue = newValue;
  }

  public String getTableName()
  {
    return tableName;
  }

  public String getKeyColumn()
  {
    return keyColumn;
  }

  public String getValueColumn()
  {
    return valueColumn;
  }

  public String getKey()
  {
    return key;
  }

  public byte[] getOldValue()
  {
    return oldValue;
  }

  public byte[] getNewValue()
  {
    return newValue;
  }
}
