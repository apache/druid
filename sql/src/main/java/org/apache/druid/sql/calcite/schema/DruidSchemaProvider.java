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

import org.apache.druid.guice.LazySingleton;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.PlannerConfig;

import javax.inject.Inject;
import java.util.List;

@LazySingleton
public class DruidSchemaProvider implements SchemaProvider
{
  private final String schemaName;
  private final BrokerSegmentMetadataCache segmentMetadataCache;
  private final DruidSchemaManager druidSchemaManager;
  private final CatalogResolver catalogResolver;
  private final PlannerConfig plannerConfig;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public DruidSchemaProvider(
      @DruidSchemaName final String schemaName,
      final BrokerSegmentMetadataCache segmentMetadataCache,
      final DruidSchemaManager druidSchemaManager,
      final CatalogResolver catalogResolver,
      final PlannerConfig plannerConfig,
      final AuthorizerMapper authorizerMapper
  )
  {
    this.schemaName = schemaName;
    this.segmentMetadataCache = segmentMetadataCache;
    this.catalogResolver = catalogResolver;
    this.plannerConfig = plannerConfig;
    this.authorizerMapper = authorizerMapper;
    if (druidSchemaManager != null && !(druidSchemaManager instanceof NoopDruidSchemaManager)) {
      this.druidSchemaManager = druidSchemaManager;
    } else {
      this.druidSchemaManager = null;
    }
  }

  @Override
  public List<NamedSchema> getSchemas(AuthenticationResult authenticationResult)
  {
    return List.of(
        new NamedDruidSchema(
            new DruidSchema(
                segmentMetadataCache,
                druidSchemaManager,
                catalogResolver,
                authorizerMapper,
                authenticationResult,
                plannerConfig.isAuthorizeTableVisibility()
            ),
            schemaName
        )
    );
  }

  /**
   * Returns the underlying metadata cache used by this instance. Not filtered based on authorization, so this
   * should only be used by code that is applying authorization filters to the table list some other way.
   */
  public BrokerSegmentMetadataCache getSegmentMetadataCache()
  {
    return segmentMetadataCache;
  }
}
