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

package org.apache.druid.sql.calcite.schema;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.view.ViewManager;

import java.util.List;

public class ViewSchemaProvider implements SchemaProvider
{
  private final ViewManager viewManager;
  private final AuthorizerMapper authorizerMapper;
  private final PlannerConfig plannerConfig;

  @Inject
  public ViewSchemaProvider(
      final ViewManager viewManager,
      final AuthorizerMapper authorizerMapper,
      final PlannerConfig plannerConfig
  )
  {
    this.viewManager = Preconditions.checkNotNull(viewManager, "viewManager");
    this.authorizerMapper = Preconditions.checkNotNull(authorizerMapper, "authorizerMapper");
    this.plannerConfig = Preconditions.checkNotNull(plannerConfig, "plannerConfig");
  }

  @Override
  public List<NamedSchema> getSchemas(AuthenticationResult authenticationResult)
  {
    return List.of(
        new NamedViewSchema(
            new ViewSchema(
                viewManager,
                authorizerMapper,
                authenticationResult,
                plannerConfig.isAuthorizeTableVisibility()
            )
        )
    );
  }
}
