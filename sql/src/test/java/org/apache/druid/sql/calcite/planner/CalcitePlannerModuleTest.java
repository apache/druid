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

package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.schema.DruidSchemaName;
import org.apache.druid.sql.calcite.schema.NamedSchema;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Properties;
import java.util.Set;

@RunWith(EasyMockRunner.class)
public class CalcitePlannerModuleTest extends CalciteTestBase
{
  private static final String SCHEMA_1 = "SCHEMA_1";
  private static final String SCHEMA_2 = "SCHEMA_2";
  private static final String DRUID_SCHEMA_NAME = "DRUID_SCHEMA_NAME";

  @Mock
  private NamedSchema druidSchema1;
  @Mock
  private NamedSchema druidSchema2;
  @Mock
  private Schema schema1;
  @Mock
  private Schema schema2;
  @Mock
  private QueryLifecycleFactory queryLifecycleFactory;
  @Mock
  private ExprMacroTable macroTable;
  @Mock
  private AuthorizerMapper authorizerMapper;
  @Mock
  private SchemaPlus rootSchema;

  private Set<SqlAggregator> aggregators;
  private Set<SqlOperatorConversion> operatorConversions;
  private Set<NamedSchema> calciteSchemas;

  private CalcitePlannerModule target;
  private Properties properties;
  private Injector injector;

  @Before
  public void setUp()
  {
    EasyMock.expect(druidSchema1.getSchema()).andStubReturn(schema1);
    EasyMock.expect(druidSchema2.getSchema()).andStubReturn(schema2);
    EasyMock.expect(druidSchema1.getSchemaName()).andStubReturn(SCHEMA_1);
    EasyMock.expect(druidSchema2.getSchemaName()).andStubReturn(SCHEMA_2);
    EasyMock.replay(druidSchema1, druidSchema2);
    calciteSchemas = ImmutableSet.of(druidSchema1, druidSchema2);
    aggregators = ImmutableSet.of();
    operatorConversions = ImmutableSet.of();
    target = new CalcitePlannerModule();
    properties = new Properties();
    injector = Guice.createInjector(
        new JacksonModule(),
        binder -> {
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(Key.get(new TypeLiteral<Set<NamedSchema>>(){})).toInstance(calciteSchemas);
          binder.bind(QueryLifecycleFactory.class).toInstance(queryLifecycleFactory);
          binder.bind(ExprMacroTable.class).toInstance(macroTable);
          binder.bind(AuthorizerMapper.class).toInstance(authorizerMapper);
          binder.bind(String.class).annotatedWith(DruidSchemaName.class).toInstance(DRUID_SCHEMA_NAME);
          binder.bind(Key.get(new TypeLiteral<Set<SqlAggregator>>(){})).toInstance(aggregators);
          binder.bind(Key.get(new TypeLiteral<Set<SqlOperatorConversion>>(){})).toInstance(operatorConversions);
          binder.bind(SchemaPlus.class).toInstance(rootSchema);
          binder.bind(Properties.class).toInstance(properties);
        },
        target
    );
  }

  @Test
  public void testDruidOperatorTableIsInjectable()
  {
    DruidOperatorTable operatorTable = injector.getInstance(DruidOperatorTable.class);
    Assert.assertNotNull(operatorTable);
    DruidOperatorTable other = injector.getInstance(DruidOperatorTable.class);
    Assert.assertNotSame(other, operatorTable);
  }

  @Test
  public void testPlannerFactoryIsInjectable()
  {
    PlannerFactory plannerFactory = injector.getInstance(PlannerFactory.class);
    Assert.assertNotNull(PlannerFactory.class);
    PlannerFactory other = injector.getInstance(PlannerFactory.class);
    Assert.assertNotSame(other, plannerFactory);
  }

  @Test
  public void testDefaultPlannerConfigIsInjected()
  {
    PlannerConfig plannerConfig = injector.getInstance(PlannerConfig.class);
    Assert.assertNotNull(plannerConfig);
    Assert.assertEquals(new Period("PT1M"), plannerConfig.getMetadataRefreshPeriod());
    Assert.assertEquals(100000, plannerConfig.getMaxTopNLimit());
    Assert.assertTrue(plannerConfig.isUseApproximateCountDistinct());
    Assert.assertTrue(plannerConfig.isUseApproximateTopN());
    Assert.assertFalse(plannerConfig.isRequireTimeCondition());
    Assert.assertTrue(plannerConfig.isAwaitInitializationOnStart());
    Assert.assertEquals(DateTimeZone.UTC, plannerConfig.getSqlTimeZone());
    Assert.assertFalse(plannerConfig.isMetadataSegmentCacheEnable());
    Assert.assertEquals(60000, plannerConfig.getMetadataSegmentPollPeriod());
    Assert.assertFalse(plannerConfig.isUseParsedExprCache());
  }

  @Test
  public void testInjectPlannerConfigWithCustomProperties()
  {
    properties.setProperty(getPlannerConfigKey("metadataRefreshPeriod"), "PT10M");
    properties.setProperty(getPlannerConfigKey("maxTopNLimit"), "10");
    properties.setProperty(getPlannerConfigKey("useApproximateCountDistinct"), "false");
    properties.setProperty(getPlannerConfigKey("useApproximateTopN"), "false");
    properties.setProperty(getPlannerConfigKey("requireTimeCondition"), "true");
    properties.setProperty(getPlannerConfigKey("awaitInitializationOnStart"), "false");
    properties.setProperty(getPlannerConfigKey("sqlTimeZone"), DateTimes.inferTzFromString("Asia/Seoul").toString());
    properties.setProperty(getPlannerConfigKey("metadataSegmentCacheEnable"), "true");
    properties.setProperty(getPlannerConfigKey("metadataSegmentPollPeriod"), "20");
    properties.setProperty(getPlannerConfigKey("useParsedExprCache"), "true");
    PlannerConfig plannerConfig = injector.getInstance(PlannerConfig.class);
    Assert.assertNotNull(plannerConfig);
    Assert.assertEquals(new Period("PT10M"), plannerConfig.getMetadataRefreshPeriod());
    Assert.assertEquals(10, plannerConfig.getMaxTopNLimit());
    Assert.assertFalse(plannerConfig.isUseApproximateCountDistinct());
    Assert.assertFalse(plannerConfig.isUseApproximateTopN());
    Assert.assertTrue(plannerConfig.isRequireTimeCondition());
    Assert.assertFalse(plannerConfig.isAwaitInitializationOnStart());
    Assert.assertEquals(DateTimes.inferTzFromString("Asia/Seoul"), plannerConfig.getSqlTimeZone());
    Assert.assertTrue(plannerConfig.isMetadataSegmentCacheEnable());
    Assert.assertEquals(20, plannerConfig.getMetadataSegmentPollPeriod());
    Assert.assertTrue(plannerConfig.isUseParsedExprCache());
  }

  private static String getPlannerConfigKey(String suffix)
  {
    return StringUtils.format("%s.%s", CalcitePlannerModule.PLANNER_CONFIG_PREFIX, suffix);
  }
}
