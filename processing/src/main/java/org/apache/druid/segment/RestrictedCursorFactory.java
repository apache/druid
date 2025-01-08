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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.policy.Policy;
import org.apache.druid.query.policy.RowFilterPolicy;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.AndFilter;

import javax.annotation.Nullable;

public class RestrictedCursorFactory implements CursorFactory
{
  private final CursorFactory delegate;
  private final Policy policy;

  public RestrictedCursorFactory(
      CursorFactory delegate,
      Policy policy
  )
  {
    this.delegate = delegate;
    this.policy = policy;
  }

  @Override
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    if (policy instanceof NoRestrictionPolicy) {
      return delegate.makeCursorHolder(spec);
    } else if (policy instanceof RowFilterPolicy) {
      final Filter rowFilter = ((RowFilterPolicy) policy).getRowFilter().toFilter();
      final CursorBuildSpec.CursorBuildSpecBuilder buildSpecBuilder = CursorBuildSpec.builder(spec);
      final Filter newFilter = spec.getFilter() == null
                               ? rowFilter
                               : new AndFilter(ImmutableList.of(spec.getFilter(), rowFilter));
      buildSpecBuilder.setFilter(newFilter);

      return delegate.makeCursorHolder(buildSpecBuilder.build());
    } else {
      throw DruidException.defensive("not supported policy type [%s]", policy.getClass());
    }
  }

  @Override
  public RowSignature getRowSignature()
  {
    return delegate.getRowSignature();
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return delegate.getColumnCapabilities(column);
  }
}
