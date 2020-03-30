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

import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.http.SqlParameter;
import org.junit.BeforeClass;

import java.util.List;

public abstract class CalciteTestBase
{
  public static final List<SqlParameter> DEFAULT_PARAMETERS = ImmutableList.of();

  @BeforeClass
  public static void setupCalciteProperties()
  {
    Calcites.setSystemProperties();
    NullHandling.initializeForTests();
  }
}
