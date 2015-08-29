/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.realtime.plumber;

import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class IntervalStartVersioningPolicyTest
{
  @Test
  public void testGetVersion() throws Exception
  {
    IntervalStartVersioningPolicy policy = new IntervalStartVersioningPolicy();
    String version = policy.getVersion(new Interval("2013-01-01/2013-01-02"));
    Assert.assertEquals("2013-01-01T00:00:00.000Z", version);
  }
}
