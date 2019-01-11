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

package org.apache.druid.sql.guice;

import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;

public class SqlBindings
{
  public static void addAggregator(
      final Binder binder,
      final Class<? extends SqlAggregator> aggregatorClass
  )
  {
    Multibinder.newSetBinder(binder, SqlAggregator.class).addBinding().to(aggregatorClass);
  }

  public static void addOperatorConversion(
      final Binder binder,
      final Class<? extends SqlOperatorConversion> clazz
  )
  {
    Multibinder.newSetBinder(binder, SqlOperatorConversion.class).addBinding().to(clazz);
  }
}
