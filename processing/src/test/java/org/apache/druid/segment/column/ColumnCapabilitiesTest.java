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

import org.apache.druid.segment.column.ColumnCapabilities.Capable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ColumnCapabilitiesTest
{
  @Test
  public void testCapableAnd()
  {
    Assertions.assertEquals(Capable.TRUE, Capable.TRUE.and(Capable.TRUE));
    Assertions.assertEquals(Capable.FALSE, Capable.TRUE.and(Capable.FALSE));
    Assertions.assertEquals(Capable.UNKNOWN, Capable.TRUE.and(Capable.UNKNOWN));

    Assertions.assertEquals(Capable.FALSE, Capable.FALSE.and(Capable.TRUE));
    Assertions.assertEquals(Capable.FALSE, Capable.FALSE.and(Capable.FALSE));
    Assertions.assertEquals(Capable.FALSE, Capable.FALSE.and(Capable.UNKNOWN));

    Assertions.assertEquals(Capable.UNKNOWN, Capable.UNKNOWN.and(Capable.TRUE));
    Assertions.assertEquals(Capable.FALSE, Capable.UNKNOWN.and(Capable.FALSE));
    Assertions.assertEquals(Capable.UNKNOWN, Capable.UNKNOWN.and(Capable.UNKNOWN));
  }

  @Test
  public void testCapableOr()
  {
    Assertions.assertEquals(Capable.TRUE, Capable.TRUE.or(Capable.TRUE));
    Assertions.assertEquals(Capable.TRUE, Capable.TRUE.or(Capable.FALSE));
    Assertions.assertEquals(Capable.TRUE, Capable.TRUE.or(Capable.UNKNOWN));

    Assertions.assertEquals(Capable.TRUE, Capable.FALSE.or(Capable.TRUE));
    Assertions.assertEquals(Capable.FALSE, Capable.FALSE.or(Capable.FALSE));
    Assertions.assertEquals(Capable.UNKNOWN, Capable.FALSE.or(Capable.UNKNOWN));

    Assertions.assertEquals(Capable.TRUE, Capable.UNKNOWN.or(Capable.TRUE));
    Assertions.assertEquals(Capable.UNKNOWN, Capable.UNKNOWN.or(Capable.FALSE));
    Assertions.assertEquals(Capable.UNKNOWN, Capable.UNKNOWN.or(Capable.UNKNOWN));
  }

  @Test
  public void testCapableOfBoolean()
  {
    Assertions.assertEquals(Capable.TRUE, Capable.of(true));
    Assertions.assertEquals(Capable.FALSE, Capable.of(false));
  }
}
