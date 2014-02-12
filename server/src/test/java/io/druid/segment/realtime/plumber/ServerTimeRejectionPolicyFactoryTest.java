/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.realtime.plumber;

import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.Test;

/**
 */
public class ServerTimeRejectionPolicyFactoryTest
{
  @Test
  public void testAccept() throws Exception
  {
    Period period = new Period("PT10M");

    RejectionPolicy rejectionPolicy = new ServerTimeRejectionPolicyFactory().create(period);

    DateTime now = new DateTime();
    DateTime past = now.minus(period).minus(100);
    DateTime future = now.plus(period).plus(100);

    Assert.assertTrue(rejectionPolicy.accept(now.getMillis()));
    Assert.assertFalse(rejectionPolicy.accept(past.getMillis()));
    Assert.assertFalse(rejectionPolicy.accept(future.getMillis()));
  }
}
