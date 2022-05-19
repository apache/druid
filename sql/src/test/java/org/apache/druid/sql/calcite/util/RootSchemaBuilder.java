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

package org.apache.druid.sql.calcite.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.calcite.TestQueryMakerFactory;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.run.QueryMakerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.DruidSchemaManager;
import org.apache.druid.sql.calcite.schema.InformationSchema;
import org.apache.druid.sql.calcite.schema.NamedDruidSchema;
import org.apache.druid.sql.calcite.schema.NamedLookupSchema;
import org.apache.druid.sql.calcite.schema.NamedSchema;
import org.apache.druid.sql.calcite.schema.NamedSystemSchema;
import org.apache.druid.sql.calcite.schema.NamedViewSchema;
import org.apache.druid.sql.calcite.schema.NoopDruidSchemaManager;
import org.apache.druid.sql.calcite.schema.SystemSchema;
import org.apache.druid.sql.calcite.schema.ViewSchema;
import org.apache.druid.sql.calcite.view.ViewManager;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RootSchemaBuilder
{
  public interface DruidSchemaCreator
  {
    DruidSchema create(
        QueryRunnerFactoryConglomerate conglomerate,
        SpecificSegmentsQuerySegmentWalker walker,
        PlannerConfig plannerConfig,
        @Nullable DruidSchemaManager druidSchemaManager);
  }

  public static class MockDruidSchemaCreator implements DruidSchemaCreator
  {
    @Override
    public DruidSchema create(
        QueryRunnerFactoryConglomerate conglomerate,
        SpecificSegmentsQuerySegmentWalker walker,
        PlannerConfig plannerConfig,
        @Nullable final DruidSchemaManager druidSchemaManager
    )
    {
      return CalciteTests.createMockSchema(
          conglomerate,
          walker,
          plannerConfig,
          druidSchemaManager);
    }
  }

  public static class CatalogResult
  {
    public final DruidSchemaCatalog catalog;
    public final QueryRunnerFactoryConglomerate conglomerate;
    public final SpecificSegmentsQuerySegmentWalker walker;

    public CatalogResult(
        DruidSchemaCatalog catalog,
        QueryRunnerFactoryConglomerate conglomerate,
        SpecificSegmentsQuerySegmentWalker walker
    )
    {
      this.catalog = catalog;
      this.conglomerate = conglomerate;
      this.walker = walker;
    }

    public QueryMakerFactory createQueryMakerFactory(ObjectMapper mapper)
    {
      return new TestQueryMakerFactory(
            CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
            mapper
            );
    }
  }

  private final PlannerConfig plannerConfig;
  private final AuthorizerMapper authorizerMapper;
  private QueryRunnerFactoryConglomerate conglomerate;
  private SpecificSegmentsQuerySegmentWalker walker;
  private DruidSchemaCreator schemaCreator;
  private DruidSchemaManager druidSchemaManager;
  private int minTopNThreshold;
  private Closer resourceCloser;
  private File temporaryFolder;
  private boolean withLookupSchema;
  private ViewManager viewManager;
  private List<NamedSchema> extraSchemas = new ArrayList<>();

  public RootSchemaBuilder(
      final PlannerConfig plannerConfig,
      final AuthorizerMapper authorizerMapper
  )
  {
    this.plannerConfig = plannerConfig;
    this.authorizerMapper = authorizerMapper;
  }

  public static RootSchemaBuilder basicBuilder(
      final PlannerConfig plannerConfig,
      final AuthorizerMapper authorizerMapper,
      final File temporaryFolder,
      final Closer resourceCloser,
      final int minTopNThreshold
  )
  {
    return new RootSchemaBuilder(plannerConfig, authorizerMapper)
        .temporaryFolder(temporaryFolder)
        .resourceCloser(resourceCloser)
        .minTopNThreshold(minTopNThreshold)
        .withLookupSchema(true)
        .resourceCloser(resourceCloser);
  }

  public RootSchemaBuilder druidSchemaManager(final DruidSchemaManager druidSchemaManager)
  {
    this.druidSchemaManager = druidSchemaManager;
    return this;
  }

  public RootSchemaBuilder congolomerate(QueryRunnerFactoryConglomerate conglomerate)
  {
    this.conglomerate = conglomerate;
    return this;
  }

  public RootSchemaBuilder walker(SpecificSegmentsQuerySegmentWalker walker)
  {
    this.walker = walker;
    return this;
  }

  public RootSchemaBuilder schemaManager(DruidSchemaManager druidSchemaManager)
  {
    this.druidSchemaManager = druidSchemaManager;
    return this;
  }

  public RootSchemaBuilder minTopNThreshold(int minTopNThreshold)
  {
    this.minTopNThreshold = minTopNThreshold;
    return this;
  }

  public RootSchemaBuilder resourceCloser(Closer resourceCloser)
  {
    this.resourceCloser = resourceCloser;
    return this;
  }

  public RootSchemaBuilder temporaryFolder(File temporaryFolder)
  {
    this.temporaryFolder = temporaryFolder;
    return this;
  }

  public RootSchemaBuilder withLookupSchema(boolean withLookupSchema)
  {
    this.withLookupSchema = withLookupSchema;
    return this;
  }

  public RootSchemaBuilder schemaCreator(DruidSchemaCreator schemaCreator)
  {
    this.schemaCreator = schemaCreator;
    return this;
  }

  public RootSchemaBuilder viewManager(ViewManager viewManager)
  {
    this.viewManager = viewManager;
    return this;
  }

  public RootSchemaBuilder withSchema(NamedSchema schema)
  {
    this.extraSchemas.add(schema);
    return this;
  }

  public CatalogResult build()
  {
    return new CatalogBuilder(this).build();
  }

  public static class CatalogBuilder
  {
    private final RootSchemaBuilder builder;
    private final Set<NamedSchema> namedSchemas = new HashSet<>();
    private final SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();
    private QueryRunnerFactoryConglomerate conglomerate;
    private SpecificSegmentsQuerySegmentWalker walker;
    private DruidSchema druidSchema;

    public CatalogBuilder(RootSchemaBuilder builder)
    {
      this.builder = builder;
    }

    public CatalogResult build()
    {
      buildDruidSchema();
      buildSystemSchema();
      buildLookupSchema();
      buildViewSchema();

      for (NamedSchema schema : builder.extraSchemas) {
        add(schema);
      }

      return buildCatalog();
    }

    private void buildDruidSchema()
    {
      conglomerate = builder.conglomerate;
      if (conglomerate == null) {
        Closer resourceCloser = builder.resourceCloser;
        if (resourceCloser == null) {
          resourceCloser = Closer.create();
        }
        int minTopNThreshold = builder.minTopNThreshold;
        if (minTopNThreshold == 0) {
          throw new ISE("minTopNThreshold is required");
        }
        conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(
            resourceCloser,
            () -> minTopNThreshold);
      }
      walker = builder.walker;
      if (walker == null) {
        if (builder.temporaryFolder == null) {
          throw new ISE("Temporary folder is required");
        }
        walker = CalciteTests.createMockWalker(
            conglomerate,
            builder.temporaryFolder
        );
      }
      DruidSchemaManager druidSchemaManager = builder.druidSchemaManager;
      if (druidSchemaManager == null) {
        druidSchemaManager = new NoopDruidSchemaManager();
      }
      DruidSchemaCreator schemaCreator = builder.schemaCreator;
      if (schemaCreator == null) {
        schemaCreator = new MockDruidSchemaCreator();
      }
      druidSchema = schemaCreator.create(
          conglomerate,
          walker,
          builder.plannerConfig,
          druidSchemaManager);
      add(new NamedDruidSchema(druidSchema, CalciteTests.DRUID_SCHEMA_NAME));
    }

    private void buildSystemSchema()
    {
      SystemSchema systemSchema = CalciteTests.createMockSystemSchema(
          druidSchema,
          walker,
          builder.plannerConfig,
          builder.authorizerMapper);
      add(new NamedSystemSchema(builder.plannerConfig, systemSchema));
    }

    private void buildLookupSchema()
    {
      if (builder.withLookupSchema) {
        add(new NamedLookupSchema(CalciteTests.createMockLookupSchema()));
      }
    }

    private void buildViewSchema()
    {
      if (builder.viewManager != null) {
        ViewSchema viewSchema = new ViewSchema(builder.viewManager);
        add(new NamedViewSchema(viewSchema));
      }
    }
    private CatalogResult buildCatalog()
    {
      DruidSchemaCatalog catalog = new DruidSchemaCatalog(
          rootSchema,
          namedSchemas.stream().collect(Collectors.toMap(NamedSchema::getSchemaName, x -> x))
      );
      InformationSchema informationSchema =
          new InformationSchema(
              catalog,
              builder.authorizerMapper
          );
      rootSchema.add(CalciteTests.INFORMATION_SCHEMA_NAME, informationSchema);
      return new CatalogResult(
          catalog,
          conglomerate,
          walker);
    }

    private void add(NamedSchema schema)
    {
      namedSchemas.add(schema);
      rootSchema.add(schema.getSchemaName(), schema.getSchema());
    }
  }
}
