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

/**
 * Test version of the fragment factory which enables Query NG only if
 * the {@code druid.queryng.enable} system property is set, and then,
 * only for scan queries.
 */
public class TestQueryManagerFactory implements QueryManagerFactory
{
  private static final String ENABLED_KEY = QueryNGConfig.CONFIG_ROOT + ".enabled";

  private boolean enabled;

  public TestQueryManagerFactory()
  {
    this.enabled = Boolean.parseBoolean(System.getProperty(ENABLED_KEY));
  }

  public TestQueryManagerFactory(boolean enabled)
  {
    this.enabled = enabled;
  }

  @Override
  public QueryManager create(Query<?> query)
  {
    if (!enabled) {
      return null;
    }
    return new QueryManager(query);
  }
}
