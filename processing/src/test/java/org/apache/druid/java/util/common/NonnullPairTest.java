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

package org.apache.druid.java.util.common;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class NonnullPairTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(NonnullPair.class).withNonnullFields("lhs", "rhs").usingGetClass().verify();
  }

  @Test
  public void testConstructorWithNull()
  {
    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("lhs");
    //noinspection ResultOfObjectAllocationIgnored
    new NonnullPair<>(null, "nullTest");
  }

  @Test
  public void testGets()
  {
    final NonnullPair<Integer, Integer> pair = new NonnullPair<>(20, 30);
    Assert.assertEquals(20, pair.lhs.intValue());
    Assert.assertEquals(30, pair.rhs.intValue());
  }
}
