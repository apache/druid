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

package org.apache.druid.guice;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.aggregation.bloom.sql.BloomFilterSqlAggregator;
import org.apache.druid.query.expressions.BloomFilterExprMacro;
import org.apache.druid.query.filter.sql.BloomFilterOperatorConversion;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.Collections;
import java.util.List;

public class BloomFilterExtensionModule implements DruidModule
{

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(new BloomFilterSerializersModule());
  }

  @Override
  public void configure(Binder binder)
  {
    SqlBindings.addOperatorConversion(binder, BloomFilterOperatorConversion.class);
    SqlBindings.addAggregator(binder, BloomFilterSqlAggregator.class);
    ExpressionModule.addExprMacro(binder, BloomFilterExprMacro.class);
  }
}
