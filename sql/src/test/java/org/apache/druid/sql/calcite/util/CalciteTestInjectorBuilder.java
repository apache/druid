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

import com.google.inject.Injector;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.SegmentWranglerModule;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.sql.calcite.aggregation.SqlAggregationModule;
import org.apache.druid.sql.calcite.util.testoperator.CalciteTestOperatorModule;

/**
 * Create the injector used for {@link CalciteTests#INJECTOR}, but in a way
 * that is extensible.
 */
public class CalciteTestInjectorBuilder extends CoreInjectorBuilder
{
  public CalciteTestInjectorBuilder()
  {
    super(new StartupInjectorBuilder()
              .withEmptyProperties()
              .build());
    add(
        new ExpressionModule(),
        new SegmentWranglerModule(),
        new LookylooModule(),
        new SqlAggregationModule(),
        new CalciteTestOperatorModule()
    );
  }

  @Override
  public Injector build()
  {
    try {
      return super.build();
    }
    catch (Exception e) {
      // Catches failures when used as a static initializer.
      e.printStackTrace();
      System.exit(1);
      throw e;
    }
  }
}
