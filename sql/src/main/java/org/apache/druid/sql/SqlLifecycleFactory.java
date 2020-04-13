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

package org.apache.druid.sql;

import com.google.inject.Inject;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.sql.calcite.planner.PlannerFactory;

@LazySingleton
public class SqlLifecycleFactory
{
  private final PlannerFactory plannerFactory;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;

  @Inject
  public SqlLifecycleFactory(
      PlannerFactory plannerFactory,
      ServiceEmitter emitter,
      RequestLogger requestLogger
  )
  {
    this.plannerFactory = plannerFactory;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
  }

  public SqlLifecycle factorize()
  {
    return new SqlLifecycle(
        plannerFactory,
        emitter,
        requestLogger,
        System.currentTimeMillis(),
        System.nanoTime()
    );
  }
}
