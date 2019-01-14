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

package org.apache.druid.sql.calcite.view;

import com.google.inject.Inject;
import org.apache.calcite.schema.TableMacro;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.planner.PlannerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * View manager that stores all views in-process. Not meant for serious usage, since views are not saved nor
 * are they shared across processes.
 */
public class InProcessViewManager implements ViewManager
{
  private final ConcurrentMap<String, DruidViewMacro> views;
  private final Escalator escalator;

  @Inject
  public InProcessViewManager(
      final Escalator escalator
  )
  {
    this.views = new ConcurrentHashMap<>();
    this.escalator = escalator;
  }

  @Override
  public void createView(final PlannerFactory plannerFactory, final String viewName, final String viewSql)
  {
    final TableMacro oldValue = views.putIfAbsent(viewName, new DruidViewMacro(plannerFactory, escalator, viewSql));
    if (oldValue != null) {
      throw new ISE("View[%s] already exists", viewName);
    }
  }

  @Override
  public void alterView(final PlannerFactory plannerFactory, final String viewName, final String viewSql)
  {
    final TableMacro oldValue = views.replace(viewName, new DruidViewMacro(plannerFactory, escalator, viewSql));
    if (oldValue != null) {
      throw new ISE("View[%s] does not exist", viewName);
    }
  }

  @Override
  public void dropView(final String viewName)
  {
    final TableMacro oldValue = views.remove(viewName);
    if (oldValue == null) {
      throw new ISE("View[%s] does not exist", viewName);
    }
  }

  @Override
  public Map<String, DruidViewMacro> getViews()
  {
    return views;
  }
}
