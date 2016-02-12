/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.lookup.jdbc;


import com.google.common.collect.ImmutableSet;
import io.dropwizard.jdbi.ImmutableSetContainerFactory;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.customizers.Define;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterContainerMapper;
import org.skife.jdbi.v2.sqlobject.stringtemplate.UseStringTemplate3StatementLocator;
import org.skife.jdbi.v2.unstable.BindIn;

import java.util.List;
import java.util.Map;


@UseStringTemplate3StatementLocator()
@RegisterContainerMapper(ImmutableSetContainerFactory.class)
public interface QueryKeys
{
  @SqlQuery("SELECT <keyColumn>, <valueColumn> FROM <table> WHERE <keyColumn> IN (<keys>)")
  ImmutableSet<Map.Entry<String, String>> findNamesForIds(
      @BindIn("keys") List<String> keys,
      @Define("table") String table,
      @Define("keyColumn") String keyColumn,
      @Define("valueColumn") String valueColumn
  );
}

