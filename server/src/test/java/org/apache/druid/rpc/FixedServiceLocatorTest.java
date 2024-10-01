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
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class FixedServiceLocatorTest
{
  public static final DruidServerMetadata DATA_SERVER_1 = new DruidServerMetadata(
      "TestDataServer",
      "hostName:9092",
      null,
      2,
      ServerType.REALTIME,
      "tier1",
      2
  );

  public static final DruidServerMetadata DATA_SERVER_2 = new DruidServerMetadata(
      "TestDataServer",
      "hostName:8083",
      null,
      2,
      ServerType.REALTIME,
      "tier1",
      2
  );

  @Test
  public void test_constructor_rejectsNull()
  {
    Assert.assertThrows(
        NullPointerException.class,
        () -> new FixedServiceLocator((ServiceLocation) null)
    );

    Assert.assertThrows(
        NullPointerException.class,
        () -> new FixedServiceLocator((ServiceLocations) null)
    );
  }

  @Test
  public void test_locate_singleServer() throws ExecutionException, InterruptedException
  {
    FixedServiceLocator serviceLocator =
        new FixedServiceLocator(ServiceLocation.fromDruidServerMetadata(DATA_SERVER_1));

    Assert.assertEquals(
        ServiceLocations.forLocation(ServiceLocation.fromDruidServerMetadata(DATA_SERVER_1)),
        serviceLocator.locate().get()
    );
  }

  @Test
  public void test_locate_afterClose() throws ExecutionException, InterruptedException
  {
    FixedServiceLocator serviceLocator =
        new FixedServiceLocator(ServiceLocation.fromDruidServerMetadata(DATA_SERVER_1));

    serviceLocator.close();

    Assert.assertEquals(
        ServiceLocations.closed(),
        serviceLocator.locate().get()
    );
  }

  @Test
  public void test_locate_multipleServers() throws ExecutionException, InterruptedException
  {
    final ServiceLocations locations = ServiceLocations.forLocations(
        ImmutableSet.of(
            ServiceLocation.fromDruidServerMetadata(DATA_SERVER_1),
            ServiceLocation.fromDruidServerMetadata(DATA_SERVER_2)
        )
    );

    FixedServiceLocator serviceLocator = new FixedServiceLocator(locations);
    Assert.assertEquals(locations, serviceLocator.locate().get());
  }
}
