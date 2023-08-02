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

package org.apache.druid.iceberg.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;


public class IcebergEqualsFilter implements IcebergFilter
{

  @JsonProperty
  private final String filterColumn;

  @JsonProperty
  private final String filterValue;

  @JsonCreator
  public IcebergEqualsFilter(
      @JsonProperty("filterColumn") String filterColumn,
      @JsonProperty("filterValue") String filterValue
  )
  {
    Preconditions.checkNotNull(filterColumn, "You must specify a filter column on the equals filter");
    Preconditions.checkNotNull(filterValue, "You must specify a filter value on the equals filter");
    this.filterColumn = filterColumn;
    this.filterValue = filterValue;
  }

  @Override
  public TableScan filter(TableScan tableScan)
  {
    return tableScan.filter(getFilterExpression());
  }

  @Override
  public Expression getFilterExpression()
  {
    return Expressions.equal(
        filterColumn,
        filterValue
    );
  }
}
