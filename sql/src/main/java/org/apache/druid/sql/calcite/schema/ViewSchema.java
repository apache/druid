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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.view.DruidViewMacro;
import org.apache.druid.sql.calcite.view.ViewManager;

import java.util.Map;
import java.util.Set;

public class ViewSchema extends AbstractSchema
{
  private final ViewManager viewManager;
  private final AuthorizerMapper authorizerMapper;
  private final AuthenticationResult authenticationResult;
  private final boolean authorizeTableVisibility;
  private final Supplier<Multimap<String, Function>> functionMultimap =
      Suppliers.memoize(this::computeFunctionMultimap);

  public ViewSchema(
      final ViewManager viewManager,
      final AuthorizerMapper authorizerMapper,
      final AuthenticationResult authenticationResult,
      final boolean authorizeTableVisibility
  )
  {
    this.viewManager = Preconditions.checkNotNull(viewManager, "viewManager");
    this.authorizerMapper = Preconditions.checkNotNull(authorizerMapper, "authorizerMapper");
    this.authenticationResult = Preconditions.checkNotNull(authenticationResult, "authenticationResult");
    this.authorizeTableVisibility = authorizeTableVisibility;
  }

  @Override
  protected Multimap<String, Function> getFunctionMultimap()
  {
    return functionMultimap.get();
  }

  private Multimap<String, Function> computeFunctionMultimap()
  {
    final Map<String, DruidViewMacro> viewsMap = viewManager.getViews();
    final Set<String> visibleViews;
    if (authorizeTableVisibility) {
      visibleViews = SchemaUtils.filterVisibleTables(
          authorizerMapper,
          authenticationResult,
          viewsMap.keySet(),
          ResourceType.VIEW
      );
    } else {
      visibleViews = viewsMap.keySet();
    }

    final ImmutableMultimap.Builder<String, Function> builder = ImmutableMultimap.builder();
    for (Map.Entry<String, DruidViewMacro> entry : viewsMap.entrySet()) {
      if (visibleViews.contains(entry.getKey())) {
        builder.put(entry);
      }
    }
    return builder.build();
  }
}
