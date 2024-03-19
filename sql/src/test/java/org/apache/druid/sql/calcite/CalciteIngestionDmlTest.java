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

package org.apache.druid.sql.calcite;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.metadata.input.InputSourceModule;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.external.ExternalOperatorConversion;
import org.apache.druid.sql.calcite.external.HttpOperatorConversion;
import org.apache.druid.sql.calcite.external.InlineOperatorConversion;
import org.apache.druid.sql.calcite.external.LocalOperatorConversion;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.guice.SqlBindings;
import org.apache.druid.sql.http.SqlParameter;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.AfterEach;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;

public class CalciteIngestionDmlTest extends BaseCalciteQueryTest
{
  protected static final Map<String, Object> DEFAULT_CONTEXT =
      ImmutableMap.<String, Object>builder()
                  .put(QueryContexts.CTX_SQL_QUERY_ID, DUMMY_SQL_ID)
                  .build();

  public static final Map<String, Object> PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT = ImmutableMap.of(
      DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY,
      "{\"type\":\"all\"}"
  );


  protected static final RowSignature FOO_TABLE_SIGNATURE =
      RowSignature.builder()
                  .addTimeColumn()
                  .add("dim1", ColumnType.STRING)
                  .add("dim2", ColumnType.STRING)
                  .add("dim3", ColumnType.STRING)
                  .add("cnt", ColumnType.LONG)
                  .add("m1", ColumnType.FLOAT)
                  .add("m2", ColumnType.DOUBLE)
                  .add("unique_dim1", HyperUniquesAggregatorFactory.TYPE)
                  .build();

  protected final ExternalDataSource externalDataSource = new ExternalDataSource(
      new InlineInputSource("a,b,1\nc,d,2\n"),
      new CsvInputFormat(ImmutableList.of("x", "y", "z"), null, false, false, 0),
      RowSignature.builder()
                  .add("x", ColumnType.STRING)
                  .add("y", ColumnType.STRING)
                  .add("z", ColumnType.LONG)
                  .build()
  );

  protected boolean didTest = false;

  public CalciteIngestionDmlTest()
  {
    super(IngestionTestSqlEngine.INSTANCE);
  }

  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);

    builder.addModule(new DruidModule() {

      // Clone of MSQExternalDataSourceModule since it is not
      // visible here.
      @Override
      public List<? extends Module> getJacksonModules()
      {
        return Collections.singletonList(
            new SimpleModule(getClass().getSimpleName())
                .registerSubtypes(ExternalDataSource.class)
        );
      }

      @Override
      public void configure(Binder binder)
      {
        // Nothing to do.
      }
    });

    builder.addModule(new DruidModule() {

      // Partial clone of MsqSqlModule, since that module is not
      // visible to this one.

      @Override
      public List<? extends Module> getJacksonModules()
      {
        // We want this module to bring input sources along for the ride.
        List<Module> modules = new ArrayList<>(new InputSourceModule().getJacksonModules());
        modules.add(new SimpleModule("test-module").registerSubtypes(TestFileInputSource.class));
        return modules;
      }

      @Override
      public void configure(Binder binder)
      {
        // We want this module to bring InputSourceModule along for the ride.
        binder.install(new InputSourceModule());

        // Set up the EXTERN macro.
        SqlBindings.addOperatorConversion(binder, ExternalOperatorConversion.class);

        // Enable the extended table functions for testing even though these
        // are not enabled in production in Druid 26.
        SqlBindings.addOperatorConversion(binder, HttpOperatorConversion.class);
        SqlBindings.addOperatorConversion(binder, InlineOperatorConversion.class);
        SqlBindings.addOperatorConversion(binder, LocalOperatorConversion.class);
      }
    });
  }

  @AfterEach
  public void tearDown()
  {
    // Catch situations where tests forgot to call "verify" on their tester.
    if (!didTest) {
      throw new ISE("Test was not run; did you call verify() on a tester?");
    }
  }

  protected String externSql(final ExternalDataSource externalDataSource)
  {
    ObjectMapper queryJsonMapper = queryFramework().queryJsonMapper();
    try {
      return StringUtils.format(
          "TABLE(extern(%s, %s, %s))",
          Calcites.escapeStringLiteral(queryJsonMapper.writeValueAsString(externalDataSource.getInputSource())),
          Calcites.escapeStringLiteral(queryJsonMapper.writeValueAsString(externalDataSource.getInputFormat())),
          Calcites.escapeStringLiteral(queryJsonMapper.writeValueAsString(externalDataSource.getSignature()))
      );
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  protected Map<String, Object> queryContextWithGranularity(Granularity granularity)
  {
    ObjectMapper queryJsonMapper = queryFramework().queryJsonMapper();
    String granularityString = null;
    try {
      granularityString = queryJsonMapper.writeValueAsString(granularity);
    }
    catch (JsonProcessingException e) {
      Assert.fail(e.getMessage());
    }
    return ImmutableMap.of(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY, granularityString);
  }

  protected IngestionDmlTester testIngestionQuery()
  {
    return new IngestionDmlTester();
  }

  public class IngestionDmlTester
  {
    private String sql;
    private PlannerConfig plannerConfig = new PlannerConfig();
    private Map<String, Object> queryContext = DEFAULT_CONTEXT;
    private AuthenticationResult authenticationResult = CalciteTests.REGULAR_USER_AUTH_RESULT;
    private String expectedTargetDataSource;
    private RowSignature expectedTargetSignature;
    private List<ResourceAction> expectedResources;
    private Query<?> expectedQuery;
    private Matcher<Throwable> validationErrorMatcher;
    private String expectedLogicalPlanResource;
    private List<SqlParameter> parameters;
    private AuthConfig authConfig;

    private IngestionDmlTester()
    {
      // Nothing to do.
    }

    public IngestionDmlTester sql(final String sql)
    {
      this.sql = sql;
      return this;
    }

    protected IngestionDmlTester sql(final String sqlPattern, final Object arg, final Object... otherArgs)
    {
      final Object[] args = new Object[otherArgs.length + 1];
      args[0] = arg;
      System.arraycopy(otherArgs, 0, args, 1, otherArgs.length);
      this.sql = StringUtils.format(sqlPattern, args);
      return this;
    }

    public IngestionDmlTester context(final Map<String, Object> context)
    {
      this.queryContext = context;
      return this;
    }

    public IngestionDmlTester authentication(final AuthenticationResult authenticationResult)
    {
      this.authenticationResult = authenticationResult;
      return this;
    }

    public IngestionDmlTester parameters(List<SqlParameter> parameters)
    {
      this.parameters = parameters;
      return this;
    }

    public IngestionDmlTester authConfig(AuthConfig authConfig)
    {
      this.authConfig = authConfig;
      return this;
    }

    public IngestionDmlTester expectTarget(
        final String expectedTargetDataSource,
        final RowSignature expectedTargetSignature
    )
    {
      this.expectedTargetDataSource = Preconditions.checkNotNull(expectedTargetDataSource, "expectedTargetDataSource");
      this.expectedTargetSignature = Preconditions.checkNotNull(expectedTargetSignature, "expectedTargetSignature");
      return this;
    }

    public IngestionDmlTester expectResources(final ResourceAction... expectedResources)
    {
      this.expectedResources = Arrays.asList(expectedResources);
      return this;
    }

    @SuppressWarnings("rawtypes")
    public IngestionDmlTester expectQuery(final Query expectedQuery)
    {
      this.expectedQuery = expectedQuery;
      return this;
    }

    public IngestionDmlTester expectValidationError(Matcher<Throwable> validationErrorMatcher)
    {
      this.validationErrorMatcher = validationErrorMatcher;
      return this;
    }

    public IngestionDmlTester expectValidationError(Class<? extends Throwable> clazz)
    {
      return expectValidationError(CoreMatchers.instanceOf(clazz));
    }

    public IngestionDmlTester expectValidationError(Class<? extends Throwable> clazz, String message)
    {
      return expectValidationError(
          CoreMatchers.allOf(
              CoreMatchers.instanceOf(clazz),
              ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo(message))
          )
      );
    }

    public IngestionDmlTester expectLogicalPlanFrom(String resource)
    {
      this.expectedLogicalPlanResource = resource;
      return this;
    }

    public void verify()
    {
      if (didTest) {
        // It's good form to only do one test per method.
        // This also helps us ensure that "verify" actually does get called.
        throw new ISE("Use one @Test method per tester");
      }

      didTest = true;

      if (sql == null) {
        throw new ISE("Test must have SQL statement");
      }

      try {
        log.info("SQL: %s", sql);

        if (validationErrorMatcher != null) {
          verifyValidationError();
        } else {
          verifySuccess();
        }
      }
      catch (RuntimeException e) {
        throw e;
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private void verifyValidationError()
    {
      if (expectedTargetDataSource != null) {
        throw new ISE("Test must not have expectedTargetDataSource");
      }

      if (expectedResources != null) {
        throw new ISE("Test must not have expectedResources");
      }

      if (expectedQuery != null) {
        throw new ISE("Test must not have expectedQuery");
      }

      final Throwable e = Assert.assertThrows(
          Throwable.class,
          () -> {
            getSqlStatementFactory(plannerConfig, authConfig).directStatement(sqlQuery()).execute();
          }
      );

      assertThat(e, validationErrorMatcher);
    }

    private void verifySuccess()
    {
      if (expectedTargetDataSource == null) {
        throw new ISE("Test must have expectedTargetDataSource");
      }

      if (expectedResources == null) {
        throw new ISE("Test must have expectedResources");
      }

      testBuilder()
          .sql(sql)
          .queryContext(queryContext)
          .authResult(authenticationResult)
          .parameters(parameters)
          .plannerConfig(plannerConfig)
          .authConfig(authConfig)
          .expectedResources(expectedResources)
          .run();

      String expectedLogicalPlan;
      if (expectedLogicalPlanResource != null) {
        expectedLogicalPlan = StringUtils.getResource(
            this,
            "/calcite/expected/ingest/" + expectedLogicalPlanResource + "-logicalPlan.txt"
        );
      } else {
        expectedLogicalPlan = null;
      }
      testBuilder()
          .sql(sql)
          .queryContext(queryContext)
          .authResult(authenticationResult)
          .parameters(parameters)
          .plannerConfig(plannerConfig)
          .authConfig(authConfig)
          .expectedQuery(expectedQuery)
          .expectedResults(Collections.singletonList(new Object[]{expectedTargetDataSource, expectedTargetSignature}))
          .expectedLogicalPlan(expectedLogicalPlan)
          .run();
    }

    private SqlQueryPlus sqlQuery()
    {
      return SqlQueryPlus.builder(sql)
          .context(queryContext)
          .auth(authenticationResult)
          .build();
    }
  }

  static class TestFileInputSource extends AbstractInputSource implements SplittableInputSource<File>
  {
    private final List<File> files;

    @JsonCreator
    TestFileInputSource(@JsonProperty("files") List<File> fileList)
    {
      files = fileList;
    }

    @Override
    @JsonIgnore
    @Nonnull
    public Set<String> getTypes()
    {
      throw new CalciteIngestDmlTestException("getTypes()");
    }

    @JsonProperty
    public List<File> getFiles()
    {
      return files;
    }

    @Override
    public Stream<InputSplit<File>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
    {
      return files.stream().map(InputSplit::new);
    }

    @Override
    public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
    {
      return files.size();
    }

    @Override
    public SplittableInputSource<File> withSplit(InputSplit<File> split)
    {
      return new TestFileInputSource(ImmutableList.of(split.get()));
    }

    @Override
    public boolean needsFormat()
    {
      return true;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestFileInputSource that = (TestFileInputSource) o;
      return Objects.equals(files, that.files);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(files);
    }
  }

  static class CalciteIngestDmlTestException extends RuntimeException
  {
    public CalciteIngestDmlTestException(String message)
    {
      super(message);
    }
  }
}
