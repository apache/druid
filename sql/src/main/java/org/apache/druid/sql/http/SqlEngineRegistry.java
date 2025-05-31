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

package org.apache.druid.sql.http;

import com.google.inject.Inject;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.initialization.jetty.BadRequestException;
import org.apache.druid.sql.calcite.run.SqlEngine;

import javax.validation.constraints.NotNull;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SqlEngineRegistry
{

  private final Map<String, SqlEngine> engines;

  @Inject
  public SqlEngineRegistry(Set<SqlEngine> engineSet)
  {
    engines = engineSet.stream().collect(Collectors.toMap(SqlEngine::name, engine -> engine));
  }

  @NotNull
  public SqlEngine getEngine(final String engineName)
  {
    SqlEngine engine = engines.getOrDefault(engineName == null ? QueryContexts.DEFAULT_ENGINE : engineName, null);
    if (engine == null) {
      throw new BadRequestException("Unsupported engine");
    }
    return engine;
  }

  public Set<String> getSupportedEngines()
  {
    return engines.keySet();
  }

  public Collection<SqlEngine> getAllEngines()
  {
    return engines.values();
  }
}
