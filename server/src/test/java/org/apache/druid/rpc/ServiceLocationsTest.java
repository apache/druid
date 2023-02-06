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

package org.apache.druid.rpc;

import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class ServiceLocationsTest
{
  @Test
  public void test_forLocation()
  {
    final ServiceLocation location = new ServiceLocation("h", -1, 2, "");
    final ServiceLocations locations = ServiceLocations.forLocation(location);

    Assert.assertEquals(ImmutableSet.of(location), locations.getLocations());
    Assert.assertFalse(locations.isClosed());
  }

  @Test
  public void test_forLocations()
  {
    final ServiceLocation location1 = new ServiceLocation("h", -1, 2, "");
    final ServiceLocation location2 = new ServiceLocation("h", -1, 2, "");

    final ServiceLocations locations = ServiceLocations.forLocations(ImmutableSet.of(location1, location2));

    Assert.assertEquals(ImmutableSet.of(location1, location2), locations.getLocations());
    Assert.assertFalse(locations.isClosed());
  }

  @Test
  public void test_closed()
  {
    final ServiceLocations locations = ServiceLocations.closed();

    Assert.assertEquals(Collections.emptySet(), locations.getLocations());
    Assert.assertTrue(locations.isClosed());
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(ServiceLocations.class)
                  .usingGetClass()
                  .verify();
  }
}
