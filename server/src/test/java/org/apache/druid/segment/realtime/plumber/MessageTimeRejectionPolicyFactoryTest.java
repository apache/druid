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

package org.apache.druid.segment.realtime.plumber;

import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class MessageTimeRejectionPolicyFactoryTest
{
  @Test
  public void testAccept()
  {
    Period period = new Period("PT10M");
    RejectionPolicy rejectionPolicy = new MessageTimeRejectionPolicyFactory().create(period);

    DateTime now = DateTimes.nowUtc();
    DateTime past = now.minus(period).minus(1);
    DateTime future = now.plus(period).plus(1);

    Assert.assertTrue(rejectionPolicy.accept(now.getMillis()));
    Assert.assertFalse(rejectionPolicy.accept(past.getMillis()));
    Assert.assertTrue(rejectionPolicy.accept(future.getMillis()));
    Assert.assertFalse(rejectionPolicy.accept(now.getMillis()));
  }
}
