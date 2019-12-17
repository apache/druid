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

package org.apache.druid.indexer.hbase2.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nullable;

import java.util.Arrays;

public class HBaseColumnSchema
{

  private static final String CF_DELIMITER = ":";

  private final String type;
  private final String name;
  private final byte[] columnFamiy;
  private final byte[] qualifier;
  private final String mappingName;

  @JsonCreator
  public HBaseColumnSchema(@JsonProperty("type") String type, @JsonProperty("name") String name,
      @Nullable @JsonProperty("mappingName") String mappingName)
  {
    this.type = type;
    this.name = name;
    byte[][] columnIdentifiers = Arrays.stream(name.split(CF_DELIMITER)).map(Bytes::toBytes)
        .toArray(byte[][]::new);

    columnFamiy = columnIdentifiers[0];
    qualifier = columnIdentifiers[1];

    this.mappingName = mappingName == null ? name.substring(name.indexOf(CF_DELIMITER) + 1)
        : mappingName;
  }

  @JsonProperty
  public String getType()
  {
    return type;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getMappingName()
  {
    return mappingName;
  }

  public byte[] getColumnFamiy()
  {
    return columnFamiy;
  }

  public byte[] getQualifier()
  {
    return qualifier;
  }

  @Override
  public String toString()
  {
    return "[type: " + type + ", name: " + name + ", mappingName: " + mappingName;
  }
}
