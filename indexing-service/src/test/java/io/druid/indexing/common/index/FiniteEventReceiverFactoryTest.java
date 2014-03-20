/*
 * Druid - a distributed column store.
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
 *
 * This file Copyright (C) 2014 N3TWORK, Inc. and contributed to the Druid project
 * under the Druid Corporate Contributor License Agreement.
 */

package io.druid.indexing.common.index;


import io.druid.data.input.InputRow;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.jackson.DefaultObjectMapper;
import junit.framework.TestCase;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class FiniteEventReceiverFactoryTest
{
  private final MapInputRowParser parser = new MapInputRowParser(
      new TimestampSpec("timestamp", null),
      Arrays.asList("dim1", "dim2"),
      new ArrayList<String>());
  private final Random rand = new Random();


  @Test
  public void testSimple() throws IOException
  {

    List<Map<String, Object>> events = new ArrayList<Map<String, Object>>(10);
    for (int i=0; i < 10; i++) {
      events.add(createEvent());
    }

    List<InputRow> expectedRows = new ArrayList<InputRow>(events.size());
    for (Map<String, Object> event : events) {
      expectedRows.add(parser.parse(event));
    }

    FiniteEventReceiverFirehoseFactory factory = new FiniteEventReceiverFirehoseFactory(
        "theServiceName", parser, null, new DefaultObjectMapper());

    FiniteEventReceiverFirehoseFactory.FiniteEventReceiverFirehose firehose
        = (FiniteEventReceiverFirehoseFactory.FiniteEventReceiverFirehose) factory.connect();

    firehose.addAll(events);
    firehose.finish();

    List<InputRow> actualRows = new ArrayList<InputRow>();
    while (firehose.hasMore()) {
      actualRows.add(firehose.nextRow());
    }
    firehose.close();

    TestCase.assertEquals("Round-trip events should match", expectedRows, actualRows);

  }

  private Map<String, Object> createEvent()
  {
    Map<String, Object> event = new HashMap<String, Object>();
    event.put("timestamp", new DateTime().toString());
    event.put("dim1", rand.nextLong());
    event.put("dim2", rand.nextInt());

    return event;
  }
}
