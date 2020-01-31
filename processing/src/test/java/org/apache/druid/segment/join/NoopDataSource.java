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

package org.apache.druid.segment.join;

import org.apache.druid.query.DataSource;

import java.util.List;
import java.util.Set;

/**
 * A datasource that returns nothing. Only used to test un-registered datasources.
 */
public class NoopDataSource implements DataSource
{
  @Override
  public Set<String> getTableNames()
  {
    return null;
  }

  @Override
  public List<DataSource> getChildren()
  {
    return null;
  }

  @Override
  public DataSource withChildren(List<DataSource> children)
  {
    return null;
  }

  @Override
  public boolean isCacheable()
  {
    return false;
  }

  @Override
  public boolean isGlobal()
  {
    return false;
  }

  @Override
  public boolean isConcrete()
  {
    return false;
  }
}
