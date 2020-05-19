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

package org.apache.druid.segment.column;

import org.junit.Assert;
import org.junit.Test;

public class ColumnCapabilitiesTest
{
  @Test
  public void testCapableAnd()
  {
    Assert.assertTrue(ColumnCapabilities.Capable.TRUE.and(ColumnCapabilities.Capable.TRUE).isTrue());
    Assert.assertFalse(ColumnCapabilities.Capable.TRUE.and(ColumnCapabilities.Capable.FALSE).isTrue());
    Assert.assertFalse(ColumnCapabilities.Capable.TRUE.and(ColumnCapabilities.Capable.UNKNOWN).isTrue());

    Assert.assertFalse(ColumnCapabilities.Capable.FALSE.and(ColumnCapabilities.Capable.TRUE).isTrue());
    Assert.assertFalse(ColumnCapabilities.Capable.FALSE.and(ColumnCapabilities.Capable.FALSE).isTrue());
    Assert.assertFalse(ColumnCapabilities.Capable.FALSE.and(ColumnCapabilities.Capable.UNKNOWN).isTrue());

    Assert.assertFalse(ColumnCapabilities.Capable.UNKNOWN.and(ColumnCapabilities.Capable.TRUE).isTrue());
    Assert.assertFalse(ColumnCapabilities.Capable.UNKNOWN.and(ColumnCapabilities.Capable.FALSE).isTrue());
    Assert.assertFalse(ColumnCapabilities.Capable.UNKNOWN.and(ColumnCapabilities.Capable.UNKNOWN).isTrue());
  }

  @Test
  public void testCapableOfBoolean()
  {
    Assert.assertEquals(ColumnCapabilities.Capable.TRUE, ColumnCapabilities.Capable.of(true));
    Assert.assertEquals(ColumnCapabilities.Capable.FALSE, ColumnCapabilities.Capable.of(false));
  }
}
