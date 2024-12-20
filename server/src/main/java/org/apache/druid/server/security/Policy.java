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

package org.apache.druid.server.security;

import com.google.common.collect.Iterables;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.segment.column.ColumnType;

import java.util.Collection;
import java.util.Objects;

public class Policy
{
  public static class RowPolicy
  {
    public static RowPolicy ALLOW_ALL = createPermissivePolicy(TrueDimFilter.instance());


    public enum Type
    {
      // A permissive row policy gives permission, multiple permissive policies can be combined with 'Or'
      Permissive,
      // A restrictive row policy restricts permission, multiple restrictive policies can be combined with 'And'
      Restrictive;
    }

    private final Type type;
    private final DimFilter filter;

    public RowPolicy(Type type, DimFilter filter)
    {
      this.type = type;
      this.filter = filter;
    }

    public static RowPolicy createRestrictiveTenantIdPolicy(String columnName, String tenantIdValue)
    {
      return new RowPolicy(Type.Restrictive, new EqualityFilter(columnName, ColumnType.STRING, tenantIdValue, null));
    }

    public static RowPolicy createPermissivePolicy(DimFilter filter)
    {
      return new RowPolicy(Type.Permissive, filter);
    }

    public boolean isPermissive()
    {
      return type.equals(Type.Permissive);
    }

    public DimFilter getFilter()
    {
      return this.filter;
    }

    public static DimFilter combines(Collection<RowPolicy> policies)
    {
      assert !policies.isEmpty();
      if (policies.size() == 1) {
        return Iterables.getOnlyElement(policies).getFilter();
      }

      throw DruidException.defensive("Multiple policies are not expected.");
    }

    @Override
    public boolean equals(Object object)
    {
      if (this == object) {
        return true;
      }
      if (object == null || getClass() != object.getClass()) {
        return false;
      }
      RowPolicy rowPolicy = (RowPolicy) object;
      return type == rowPolicy.type && Objects.equals(filter, rowPolicy.filter);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(type, filter);
    }

  }
}
