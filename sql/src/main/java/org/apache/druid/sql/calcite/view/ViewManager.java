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

import org.apache.druid.query.policy.Policy;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.ViewSchema;

import java.util.Map;

/**
 * View managers appear in the {@link ViewSchema}. They are not currently exposed via user-facing API, but may
 * be exposed in the future. View managers must be thread-safe.
 *
 * <p>Access to views is authorized using {@link ResourceType#VIEW}. Views are expanded by {@link DruidViewMacro}
 * using escalated privileges, not the privileges of the user running the query. This means that views are a
 * security boundary: it is possible for a user to have access to a view {@code aview} that references
 * a table {@code atable} that the user does *not* have access to.
 *
 * <p>Views are treated as owned by the superuser (superuser privileges are used for view expansion). Therefore,
 * users must not be allowed to create their own views, as this would enable them to access tables that they may
 * not otherwise have had access to.
 *
 * <p>Note that for tables reached entirely through views, policies ({@link Policy}) are not attached. This is
 * consistent with the idea that views are expanded as the superuser.
 */
public interface ViewManager
{
  void createView(PlannerFactory plannerFactory, String viewName, String viewSql);

  void alterView(PlannerFactory plannerFactory, String viewName, String viewSql);

  void dropView(String viewName);

  Map<String, DruidViewMacro> getViews();
}
