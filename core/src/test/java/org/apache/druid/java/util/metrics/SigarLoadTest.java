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

package org.apache.druid.java.util.metrics;

import org.hyperic.sigar.Sigar;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class SigarLoadTest
{
  private static final String CPU_ARCH = System.getProperty("os.arch");

  @Before
  public void before()
  {
    // Do not run the tests on ARM64. Sigar library has no binaries for ARM64
    Assume.assumeFalse("aarch64".equals(CPU_ARCH));
  }

  @Test
  public void testSigarLoad()
  {
    Sigar sigar = SigarUtil.getSigar();
    Assert.assertTrue(sigar.getPid() > 0);
  }
}
