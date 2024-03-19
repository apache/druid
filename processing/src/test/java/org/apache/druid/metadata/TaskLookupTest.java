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

package org.apache.druid.metadata;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.metadata.TaskLookup.ActiveTaskLookup;
import org.apache.druid.metadata.TaskLookup.CompleteTaskLookup;
import org.apache.druid.metadata.TaskLookup.TaskLookupType;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class TaskLookupTest
{
  public static class CompleteTaskLookupTest
  {
    @Test
    public void testEquals()
    {
      EqualsVerifier.forClass(CompleteTaskLookup.class).usingGetClass().verify();
    }

    @Test
    public void testGetType()
    {
      Assert.assertEquals(TaskLookupType.COMPLETE, CompleteTaskLookup.of(null, null).getType());
    }

    @Test
    public void testNullParams()
    {
      final CompleteTaskLookup lookup = CompleteTaskLookup.of(null, null);
      Assert.assertNull(lookup.getMaxTaskStatuses());
      Assert.assertFalse(lookup.hasTaskCreatedTimeFilter());
      Assert.assertThrows(AssertionError.class, lookup::getTasksCreatedPriorTo);
      Assert.assertFalse(lookup.isNil());
    }

    @Test
    public void testWithDurationBeforeNow()
    {
      final Duration duration = new Period("P1D").toStandardDuration();
      final DateTime timestampBeforeLookupCreated = DateTimes.nowUtc().minus(duration);
      final CompleteTaskLookup lookup = CompleteTaskLookup
          .of(null, null)
          .withMinTimestampIfAbsent(timestampBeforeLookupCreated);
      Assert.assertNull(lookup.getMaxTaskStatuses());
      Assert.assertTrue(
          timestampBeforeLookupCreated.isEqual(lookup.getTasksCreatedPriorTo())
          || timestampBeforeLookupCreated.isBefore(lookup.getTasksCreatedPriorTo())
      );
      Assert.assertFalse(lookup.isNil());
    }

    @Test
    public void testWithDurationBeforeNow2()
    {
      final Duration duration = new Period("P1D").toStandardDuration();
      final DateTime timestampBeforeLookupCreated = DateTimes.nowUtc().minus(duration);
      final CompleteTaskLookup lookup =
          new CompleteTaskLookup(null, DateTimes.of("2000"))
              .withMinTimestampIfAbsent(timestampBeforeLookupCreated);
      Assert.assertNull(lookup.getMaxTaskStatuses());
      Assert.assertEquals(
          DateTimes.of("2000"),
          lookup.getTasksCreatedPriorTo()
      );
      Assert.assertFalse(lookup.isNil());
    }

    @Test
    public void testNonNullParams()
    {
      final Duration duration = new Period("P1D").toStandardDuration();
      final DateTime timestampBeforeLookupCreated = DateTimes.nowUtc().minus(duration);
      final CompleteTaskLookup lookup = CompleteTaskLookup.of(3, duration);
      Assert.assertNotNull(lookup.getMaxTaskStatuses());
      Assert.assertEquals(3, lookup.getMaxTaskStatuses().intValue());
      Assert.assertTrue(lookup.hasTaskCreatedTimeFilter());
      Assert.assertTrue(
          timestampBeforeLookupCreated.isEqual(lookup.getTasksCreatedPriorTo())
          || timestampBeforeLookupCreated.isBefore(lookup.getTasksCreatedPriorTo())
      );
      Assert.assertFalse(lookup.isNil());
    }

    @Test
    public void testZeroStatuses()
    {
      final CompleteTaskLookup lookup = CompleteTaskLookup.of(0, null);
      Assert.assertNotNull(lookup.getMaxTaskStatuses());
      Assert.assertEquals(0, lookup.getMaxTaskStatuses().intValue());
      Assert.assertTrue(lookup.isNil());
    }
  }

  public static class ActiveTaskLookupTest
  {
    @Test
    public void testSingleton()
    {
      final ActiveTaskLookup lookup1 = ActiveTaskLookup.getInstance();
      final ActiveTaskLookup lookup2 = ActiveTaskLookup.getInstance();
      Assert.assertEquals(lookup1, lookup2);
      Assert.assertSame(lookup1, lookup2);
      Assert.assertFalse(lookup1.isNil());
      Assert.assertFalse(lookup2.isNil());
    }

    @Test
    public void testGetType()
    {
      Assert.assertEquals(TaskLookupType.ACTIVE, ActiveTaskLookup.getInstance().getType());
    }
  }
}
