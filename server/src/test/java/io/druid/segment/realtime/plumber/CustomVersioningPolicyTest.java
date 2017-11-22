/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.realtime.plumber;

import io.druid.server.ServerTestHelper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

public class CustomVersioningPolicyTest
{

  @Test
  public void testSerialization() throws Exception
  {
    Interval interval = new Interval(DateTime.now(DateTimeZone.UTC), DateTime.now(DateTimeZone.UTC));
    String version = "someversion";

    CustomVersioningPolicy policy = new CustomVersioningPolicy(version);

    CustomVersioningPolicy serialized = ServerTestHelper.MAPPER.readValue(
        ServerTestHelper.MAPPER.writeValueAsBytes(policy),
        CustomVersioningPolicy.class
    );

    Assert.assertEquals(version, policy.getVersion(interval));
    Assert.assertEquals(version, serialized.getVersion(interval));
  }
}
