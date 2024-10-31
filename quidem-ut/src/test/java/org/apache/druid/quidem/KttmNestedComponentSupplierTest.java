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

package org.apache.druid.quidem;

import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.ComponentSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

@EnabledIf(value = "isReplaceWithDefault", disabledReason = "Needed to provide coverage in defaults mode")
@ComponentSupplier(KttmNestedComponentSupplier.class)
public class KttmNestedComponentSupplierTest extends BaseCalciteQueryTest
{
  static {
    NullHandling.initializeForTests();
  }

  public static boolean isReplaceWithDefault()
  {
    return NullHandling.replaceWithDefault();
  }

  @Test
  public void testDataset()
  {
    msqIncompatible();
    testBuilder()
        .sql("SELECT count(1),sum(session_length) from kttm_nested")
        .expectedResults(ImmutableList.of(new Object[] {465346L, 153573448620L}))
        .run();
  }
}
