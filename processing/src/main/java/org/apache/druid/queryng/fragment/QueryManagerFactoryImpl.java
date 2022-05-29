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

package org.apache.druid.queryng.fragment;

import org.apache.druid.query.Query;
import org.apache.druid.queryng.config.QueryNGConfig;

import javax.inject.Inject;

/**
 * Creates a fragment context for the "shim" implementation of the
 * NG query engine, but only if the engine is enabled. Queries should
 * take the existence of the fragment context as their indication to use
 * the NG engine, else stick with the "classic" engine.
 */
public class QueryManagerFactoryImpl implements QueryManagerFactory
{
  private final QueryNGConfig config;

  @Inject
  public QueryManagerFactoryImpl(QueryNGConfig config)
  {
    this.config = config;
  }

  @Override
  public QueryManager create(final Query<?> query)
  {
    // Config imposes a number of obstacles.
    if (!config.isEnabled(query)) {
      return null;
    }
    return new QueryManager(query.getId());
  }
}
