/*
* Druid - a distributed column store.
* Copyright (C) 2012  Metamarkets Group Inc.
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
package druid.examples.conjurer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.realtime.firehose.Firehose;
import io.d8a.conjure.Conjurer.Builder;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ConjurerFirehoseFactoryTest
{
  @Test
  public void testMapIngestion() throws IOException
  {
    final Map<String, Object> map = ImmutableMap.<String, Object>of(
        "value1",
        1.0f,
        "value2",
        "two",
        "time",
        "118215000"
    );
    ConjurerFirehoseFactory factory = new ConjurerFirehoseFactory(
        new ConjurerWrapper(new Builder())
        {
          @Override
          public Map<String, Object> takeFromQueue(long waitTime, TimeUnit unit)
          {
            return map;

          }

          @Override
          public void start() {}

          ;
        }
    );
    Firehose firehose = factory.connect();
    Assert.assertEquals(true, firehose.hasMore());
    InputRow inputRow = firehose.nextRow();
    Assert.assertEquals(map.keySet(), inputRow.getDimensions());
    Assert.assertEquals(inputRow.getDimension("value2"), Lists.newArrayList(map.get("value2")));
    Assert.assertEquals(inputRow.getFloatMetric("value1"), map.get("value1"));
    Assert.assertEquals(inputRow.getTimestampFromEpoch(), new DateTime(map.get("time")).getMillis());
  }


}
