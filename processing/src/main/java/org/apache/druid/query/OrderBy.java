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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Objects;

public class OrderBy
{
  public static OrderBy ascending(String columnName)
  {
    return new OrderBy(columnName, Order.ASCENDING);
  }

  public static OrderBy descending(String columnName)
  {
    return new OrderBy(columnName, Order.DESCENDING);
  }

  private final String columnName;
  private final Order order;

  @JsonCreator
  public OrderBy(
      @JsonProperty("columnName") final String columnName,
      @JsonProperty("order") final Order order
  )
  {
    this.columnName = Preconditions.checkNotNull(columnName, "columnName");
    this.order = Preconditions.checkNotNull(order, "order");

    if (order == Order.NONE) {
      throw new IAE("Order required for column [%s]", columnName);
    }
  }

  @JsonProperty
  public String getColumnName()
  {
    return columnName;
  }

  @JsonProperty
  public Order getOrder()
  {
    return order;
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
    OrderBy that = (OrderBy) o;
    return Objects.equals(columnName, that.columnName) && order == that.order;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columnName, order);
  }

  @Override
  public String toString()
  {
    return StringUtils.format("%s %s", columnName, order == Order.ASCENDING ? "ASC" : "DESC");
  }

}
