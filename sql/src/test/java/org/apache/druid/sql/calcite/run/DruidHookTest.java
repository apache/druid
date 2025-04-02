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

package org.apache.druid.sql.calcite.run;

import com.google.gson.internal.JavaVersion;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.sql.hook.DruidHook;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class DruidHookTest
{
  @Test
  public void failOnJdk11()
  {
    int v = JavaVersion.getMajorJavaVersion();
    assertNotEquals(11, v);
  }

  @Test
  public void failOnJdk21()
  {
    int v = JavaVersion.getMajorJavaVersion();
    assertNotEquals(21, v);
  }

  @Test
  public void testHookKeyEquals()
  {
    EqualsVerifier.forClass(DruidHook.HookKey.class)
        .withNonnullFields("label", "type")
        .usingGetClass()
        .verify();
  }
}
