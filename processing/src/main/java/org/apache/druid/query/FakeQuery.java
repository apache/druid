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

import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;

import java.util.Map;

/**
 * A query that cannot be executed. Useful where a {@link Query} instance is needed but will never be run, such as
 * probing whether a component would answer queries for a datasource, or in tests.
 */
@JsonTypeName(FakeQuery.TYPE)
public class FakeQuery extends BaseQuery<Object>
{
  public static final String TYPE = "fake";

  public FakeQuery(
      final DataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, context);
  }

  @Override
  public boolean hasFilters()
  {
    return false;
  }

  @Override
  public DimFilter getFilter()
  {
    return null;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public Query<Object> withQuerySegmentSpec(final QuerySegmentSpec spec)
  {
    throw new UnsupportedOperationException("FakeQuery cannot be modified");
  }

  @Override
  public Query<Object> withDataSource(final DataSource dataSource)
  {
    throw new UnsupportedOperationException("FakeQuery cannot be modified");
  }

  @Override
  public Query<Object> withOverriddenContext(final Map<String, Object> contextOverride)
  {
    throw new UnsupportedOperationException("FakeQuery cannot be modified");
  }
}
