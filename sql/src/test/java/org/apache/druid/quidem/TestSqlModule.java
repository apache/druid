package org.apache.druid.quidem;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.ServerInjectorBuilderTest.TestDruidModule;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QuerySchedulerProvider;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.schema.DruidSchemaName;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.guice.SqlModule;

public class TestSqlModule extends TestDruidModule
{
  @Override
  public void configure(Binder binder)
  {
    binder.install(new SqlModule.SqlStatementFactoryModule());
    binder.bind(String.class)
        .annotatedWith(DruidSchemaName.class)
        .toInstance(CalciteTests.DRUID_SCHEMA_NAME);
    binder.bind(new TypeLiteral<Supplier<DefaultQueryConfig>>()
    {
    }).toInstance(Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of())));
    binder.bind(CalciteRulesManager.class).toInstance(new CalciteRulesManager(ImmutableSet.of()));
    TestRequestLogger testRequestLogger = new TestRequestLogger();
    binder.bind(RequestLogger.class).toInstance(testRequestLogger);
    binder.bind(CatalogResolver.class).toInstance(CatalogResolver.NULL_RESOLVER);
    binder.bind(ServiceEmitter.class).to(NoopServiceEmitter.class);
    binder.bind(QueryScheduler.class)
        .toProvider(QuerySchedulerProvider.class)
        .in(LazySingleton.class);
    binder.bind(QuerySchedulerProvider.class).in(LazySingleton.class);
    binder.bind(AuthenticatorMapper.class).toInstance(CalciteTests.TEST_AUTHENTICATOR_MAPPER);
    binder.bind(AuthorizerMapper.class).toInstance(CalciteTests.TEST_AUTHORIZER_MAPPER);
    binder.bind(Escalator.class).toInstance(CalciteTests.TEST_AUTHENTICATOR_ESCALATOR);
  }
}
