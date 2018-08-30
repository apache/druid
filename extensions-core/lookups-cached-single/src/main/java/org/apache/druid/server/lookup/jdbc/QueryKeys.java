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

package org.apache.druid.server.lookup.jdbc;


import com.google.common.collect.ImmutableSet;
import org.skife.jdbi.v2.ContainerBuilder;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.customizers.Define;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterContainerMapper;
import org.skife.jdbi.v2.sqlobject.stringtemplate.UseStringTemplate3StatementLocator;
import org.skife.jdbi.v2.tweak.ContainerFactory;
import org.skife.jdbi.v2.unstable.BindIn;

import java.util.List;
import java.util.Map;

@UseStringTemplate3StatementLocator()
@RegisterContainerMapper(QueryKeys.QueryKeysContainerFactory.class)
public interface QueryKeys
{
  @SqlQuery("SELECT <keyColumn>, <valueColumn> FROM <table> WHERE <keyColumn> IN (<keys>)")
  ImmutableSet<Map.Entry<String, String>> findNamesForIds(
      @BindIn("keys") List<String> keys,
      @Define("table") String table,
      @Define("keyColumn") String keyColumn,
      @Define("valueColumn") String valueColumn
  );

  class QueryKeysContainerFactory implements ContainerFactory<ImmutableSet<?>>
  {
    @Override
    public boolean accepts(Class<?> type)
    {
      return ImmutableSet.class.isAssignableFrom(type);
    }

    @Override
    public ContainerBuilder<ImmutableSet<?>> newContainerBuilderFor(Class<?> type)
    {
      return new ContainerBuilder<ImmutableSet<?>>()
      {
        final ImmutableSet.Builder<Object> builder = new ImmutableSet.Builder<>();

        @Override
        public ContainerBuilder<ImmutableSet<?>> add(final Object obj)
        {
          builder.add(obj);
          return this;
        }

        @Override
        public ImmutableSet<?> build()
        {
          return builder.build();
        }
      };
    }
  }
}
