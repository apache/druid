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
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * {@link TableDataSource} variant for globally available 'broadcast' segments. If bound to a
 * {@link org.apache.druid.segment.join.JoinableFactory} that can create an
 * {@link org.apache.druid.segment.join.table.IndexedTable} using DruidBinders.joinableFactoryBinder, this allows
 * optimal usage of segments using this DataSource type in join operations (because they are global), and so can be
 * pushed down to historicals as a {@link JoinDataSource}, instead of requiring a subquery join using
 * {@link InlineDataSource} to construct an {@link org.apache.druid.segment.join.table.IndexedTable} on the fly on the
 * broker. Because it is also a {@link TableDataSource}, when queried directly, or on the left hand side of a join,
 * they will be treated as any normal table datasource.
 */
@JsonTypeName("globalTable")
public class GlobalTableDataSource extends TableDataSource
{
  @JsonCreator
  public GlobalTableDataSource(@JsonProperty("name") String name)
  {
    super(name);
  }

  @Override
  public boolean isGlobal()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "GlobalTableDataSource{" +
           "name='" + getName() + '\'' +
           '}';
  }
}
