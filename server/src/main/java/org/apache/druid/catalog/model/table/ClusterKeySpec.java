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

package org.apache.druid.catalog.model.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Description of one clustering key (column) for a datasource. Clustering is
 * the process of physically sorting data by a sort key. This class represents
 * one column of that sort key. The key consists of a name and a sort direction.
 * Sort direction is optional: omitted, ascending is assumed.
 * (In Druid, clustering is always {@code NULLS LOW} in SQL parlance, so that attribute
 * does not appear here.
 */
public class ClusterKeySpec
{
  private final String expr;
  private final boolean desc;

  @JsonCreator
  public ClusterKeySpec(
      @JsonProperty("column") String expr,
      @JsonProperty("desc") @Nullable Boolean desc
  )
  {
    this.expr = expr;
    this.desc = desc != null && desc;
  }

  @JsonProperty("column")
  public String expr()
  {
    return expr;
  }

  @JsonProperty("desc")
  public boolean desc()
  {
    return desc;
  }

  @Override
  public String toString()
  {
    return expr + (desc ? " DESC" : "");
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || o.getClass() != getClass()) {
      return false;
    }
    ClusterKeySpec other = (ClusterKeySpec) o;
    return Objects.equals(this.expr, other.expr)
        && this.desc == other.desc;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(expr, desc);
  }
}
