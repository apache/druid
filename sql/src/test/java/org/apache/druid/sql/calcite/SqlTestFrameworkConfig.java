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

import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.HashMap;
import java.util.Map;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface SqlTestFrameworkConfig
{
  int numMergeBuffers() default 0;

  int minTopNThreshold() default TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD;

  public static class ClassRule extends ExternalResource
  {

    Map<SqlTestFrameworkConfig, SqlTestFramework> frameworkMap = new HashMap<SqlTestFrameworkConfig, SqlTestFramework>();

    public SqlTestFrameworkRule methodRule(BaseCalciteQueryTest testHost)
    {
      return new SqlTestFrameworkRule(this, testHost);
    }

    @Override
    protected void after()
    {
      for (SqlTestFramework f : frameworkMap.values()) {
        f.close();
      }
      frameworkMap.clear();
    }
  }

  public static class SqlTestFrameworkRule implements TestRule
  {
    private SqlTestFrameworkConfig config;
    private ClassRule classRule;
    private QueryComponentSupplier testHost;

    public SqlTestFrameworkRule(ClassRule classRule, QueryComponentSupplier testHost)
    {
      this.classRule = classRule;
      this.testHost = testHost;
    }

    @SqlTestFrameworkConfig
    public SqlTestFrameworkConfig defaultConfig()
    {
      try {
        return getClass()
            .getMethod("defaultConfig")
            .getAnnotation(SqlTestFrameworkConfig.class);
      }
      catch (NoSuchMethodException | SecurityException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Statement apply(Statement base, Description description)
    {
      config = description.getAnnotation(SqlTestFrameworkConfig.class);
      if (config == null) {
        config = defaultConfig();
      }
      return base;
    }

    public SqlTestFramework get()
    {
      return classRule.frameworkMap.computeIfAbsent(config, this::createFramework);
    }

    private SqlTestFramework createFramework(SqlTestFrameworkConfig config)
    {
      SqlTestFramework.Builder builder = new SqlTestFramework.Builder(testHost)
          .minTopNThreshold(config.minTopNThreshold())
          .mergeBufferCount(config.numMergeBuffers());
      return builder.build();
    }
  }
}
