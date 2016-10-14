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

package io.druid.segment.incremental;

import com.google.common.base.Function;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Assert;
import org.junit.Test;

public class TimestampFormatterTest
{
  @Test
  public void testMillis() throws Exception
  {
    Function<Long, String> formatter = TimestampFormatter.createTimestampFormatter("millis");
    Assert.assertEquals("1358347307435", formatter.apply(1358347307435L));
  }

  @Test
  public void testRuby() throws Exception
  {
    Function<Long, String> formatter = TimestampFormatter.createTimestampFormatter("ruby");
    Assert.assertEquals("1358347307.435000", formatter.apply(1358347307435L));
  }

  @Test
  public void testNano() throws Exception
  {
    Function<Long, String> formatter = TimestampFormatter.createTimestampFormatter("nano");
    assert formatter != null;
    Assert.assertEquals("1358347307435000000", formatter.apply(1358347307435L));
  }

  @Test
  public void testPosix() throws Exception
  {
    Function<Long, String> formatter = TimestampFormatter.createTimestampFormatter("posix");
    assert formatter != null;
    Assert.assertEquals("1358347307", formatter.apply(1358347307435L));
  }

  @Test
  public void testIso() throws Exception
  {
    Function<Long, String> formatter = TimestampFormatter.createTimestampFormatter("iso");
    assert formatter != null;
    Assert.assertEquals(ISODateTimeFormat.dateTime().print(1358347307435L), formatter.apply(1358347307435L));
  }

  @Test
  public void testOther() throws Exception
  {
    Function<Long, String> formatter = TimestampFormatter.createTimestampFormatter("yyyy-MM-dd");
    assert formatter != null;
    Assert.assertEquals(ISODateTimeFormat.date().print(1358347307435L), formatter.apply(1358347307435L));
  }
}
