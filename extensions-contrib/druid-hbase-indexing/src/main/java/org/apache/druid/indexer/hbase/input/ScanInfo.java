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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.guice.annotations.ExtensionPoint;

@ExtensionPoint
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = TableScanInfo.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "table", value = TableScanInfo.class),
    @JsonSubTypes.Type(name = "snapshot", value = SnapshotScanInfo.class)
})
public abstract class ScanInfo
{

  protected final String name;
  protected final String startKey;
  protected final String endKey;
  protected final String filter;

  @JsonCreator
  public ScanInfo(String name, String startKey, String endKey, String filter)
  {
    this.name = name;
    this.startKey = startKey;
    this.endKey = endKey;
    this.filter = filter;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getStartKey()
  {
    return startKey;
  }

  @JsonProperty
  public String getEndKey()
  {
    return endKey;
  }

  @JsonProperty
  public String getFilter()
  {
    return filter;
  }

  public abstract ScanInfo createNewScan(String startKey, String endKey);

  @Override
  public String toString()
  {
    return "[ type:" + this.getClass().getName() + ", " + name + ": (" + (startKey == null ? "" : startKey) + "~"
        + (endKey == null ? "" : endKey) + "), filter=" + (filter == null ? "none" : filter) + "]";
  }
}
