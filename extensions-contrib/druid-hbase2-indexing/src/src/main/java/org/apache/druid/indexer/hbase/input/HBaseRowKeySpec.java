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

package org.apache.druid.indexer.hbase.input;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.hadoop.hbase.client.Result;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

@ExtensionPoint
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "format")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "delimiter", value = DelimiterHBaseRowKeySpec.class),
    @JsonSubTypes.Type(name = "fixedLength", value = FixedLengthHBaseRowKeySpec.class) })
public abstract class HBaseRowKeySpec
{

  private final List<HBaseRowKeySchema> rowkeySchemaList;

  public HBaseRowKeySpec(List<HBaseRowKeySchema> rowkeySchemaList)
  {
    this.rowkeySchemaList = rowkeySchemaList;
  }

  @JsonProperty
  public List<HBaseRowKeySchema> getRowkeySchemaList()
  {
    return rowkeySchemaList;
  }

  protected Iterator<HBaseRowKeySchema> iterator()
  {
    return rowkeySchemaList.iterator();
  }

  public abstract Map<String, Object> getRowKeyColumns(Result result);
}
