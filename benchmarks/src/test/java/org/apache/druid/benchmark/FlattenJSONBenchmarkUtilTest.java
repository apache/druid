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

package org.apache.druid.benchmark;

import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FlattenJSONBenchmarkUtilTest
{
  private static final InputRowSchema INPUT_ROW_SCHEMA = new InputRowSchema(
      new TimestampSpec("ts", "iso", null),
      DimensionsSpec.EMPTY,
      ColumnsFilter.all()
  );

  @Test
  public void testOne() throws Exception
  {
    FlattenJSONBenchmarkUtil eventGen = new FlattenJSONBenchmarkUtil();

    byte[] flatEventBytes = StringUtils.toUtf8(eventGen.generateFlatEvent());
    byte[] nestedEventBytes = StringUtils.toUtf8(eventGen.generateNestedEvent());

    JsonInputFormat flatFormat = eventGen.getFlatFormat();
    JsonInputFormat nestedFormat = eventGen.getNestedFormat();
    JsonInputFormat jqFormat = eventGen.getJqFormat();

    InputRow event;
    InputRow event2;
    InputRow event3;

    try (CloseableIterator<InputRow> iterator = flatFormat.createReader(
        INPUT_ROW_SCHEMA,
        new ByteEntity(flatEventBytes),
        null
    ).read()) {
      event = iterator.next();
    }

    try (CloseableIterator<InputRow> iterator = nestedFormat.createReader(
        INPUT_ROW_SCHEMA,
        new ByteEntity(nestedEventBytes),
        null
    ).read()) {
      event2 = iterator.next();
    }

    try (CloseableIterator<InputRow> iterator = jqFormat.createReader(
        INPUT_ROW_SCHEMA,
        new ByteEntity(nestedEventBytes),
        null
    ).read()) {
      event3 = iterator.next(); // reuse the same event as "nested"
    }

    checkEvent1(event);
    checkEvent2(event2);
    checkEvent2(event3); // make sure JQ format output matches with JSONPath format output
  }

  public void checkEvent1(InputRow event)
  {
    Assertions.assertEquals("2015-09-12T12:10:53.155Z", event.getRaw("ts").toString());
    Assertions.assertEquals("-1170723877", event.getRaw("d1").toString());
    Assertions.assertEquals("238976084", event.getRaw("d2").toString());
    Assertions.assertEquals("0.9818780016507468", event.getRaw("m3").toString());
    Assertions.assertEquals("-3821883769350174965", event.getRaw("m4").toString());
    Assertions.assertEquals("-509091100", event.getRaw("e1.d1").toString());
    Assertions.assertEquals("274706327", event.getRaw("e1.d2").toString());
    Assertions.assertEquals("870378185", event.getRaw("e2.d3").toString());
    Assertions.assertEquals("-377775321", event.getRaw("e2.d4").toString());
    Assertions.assertEquals("-1797988763", event.getRaw("e2.d5").toString());
    Assertions.assertEquals("1309474524", event.getRaw("e2.d6").toString());
    Assertions.assertEquals("129047958", event.getRaw("e2.ad1[0]").toString());
    Assertions.assertEquals("1658972185", event.getRaw("e2.ad1[1]").toString());
    Assertions.assertEquals("-997010830", event.getRaw("e2.ad1[2]").toString());

    Assertions.assertEquals("-5877201484736882047", event.getRaw("e3.m1").toString());

    Assertions.assertEquals("0.4375433369079904", event.getRaw("e3.m2").toString());
    Assertions.assertEquals("0.8510482953607659", event.getRaw("e3.m3").toString());
    Assertions.assertEquals("-2383262648875933574", event.getRaw("e3.m4").toString());
    Assertions.assertEquals("7978976213260706704", event.getRaw("e3.am1[0]").toString());
    Assertions.assertEquals("-7863478723500557583", event.getRaw("e3.am1[1]").toString());
    Assertions.assertEquals("8737294556898244483", event.getRaw("e3.am1[2]").toString());
    Assertions.assertEquals("3192812480241489927", event.getRaw("e3.am1[3]").toString());
    Assertions.assertEquals("-3980663171371801209", event.getRaw("e4.e4.m4").toString());
    Assertions.assertEquals("-1915243040", event.getRaw("ae1[0].d1").toString());
    Assertions.assertEquals("-2020543641", event.getRaw("ae1[1].d1").toString());
    Assertions.assertEquals("1414285347", event.getRaw("ae1[2].e1.d2").toString());
  }

  public void checkEvent2(InputRow event2)
  {
    Assertions.assertEquals("728062074", event2.getRaw("ae1[0].d1").toString());
    Assertions.assertEquals("1701675101", event2.getRaw("ae1[1].d1").toString());
    Assertions.assertEquals("1887775139", event2.getRaw("ae1[2].e1.d2").toString());
    Assertions.assertEquals("1375814994", event2.getRaw("e1.d1").toString());
    Assertions.assertEquals("-1747933975", event2.getRaw("e1.d2").toString());
    Assertions.assertEquals("1616761116", event2.getRaw("e2.ad1[0]").toString());
    Assertions.assertEquals("7645432", event2.getRaw("e2.ad1[1]").toString());
    Assertions.assertEquals("679897970", event2.getRaw("e2.ad1[2]").toString());
    Assertions.assertEquals("-1797792200", event2.getRaw("e2.d3").toString());
    Assertions.assertEquals("142582995", event2.getRaw("e2.d4").toString());
    Assertions.assertEquals("-1341994709", event2.getRaw("e2.d5").toString());
    Assertions.assertEquals("-889954295", event2.getRaw("e2.d6").toString());
    Assertions.assertEquals("678995794", event2.getRaw("d1").toString());
    Assertions.assertEquals("-1744549866", event2.getRaw("d2").toString());
    Assertions.assertEquals("2015-09-12T12:10:53.155Z", event2.getRaw("ts").toString());
    Assertions.assertEquals("0.7279915615037622", event2.getRaw("m3").toString());
    Assertions.assertEquals("977083178034247050", event2.getRaw("m4").toString());
    Assertions.assertEquals("1940993614184952155", event2.getRaw("e3.m1").toString());
    Assertions.assertEquals("0.55936084127688", event2.getRaw("e3.m2").toString());
    Assertions.assertEquals("0.22821798320943232", event2.getRaw("e3.m3").toString());
    Assertions.assertEquals("8176144126231114468", event2.getRaw("e3.m4").toString());
    Assertions.assertEquals("-7405674050450245158", event2.getRaw("e3.am1[0]").toString());
    Assertions.assertEquals("150970357863018887", event2.getRaw("e3.am1[1]").toString());
    Assertions.assertEquals("3261802881806411610", event2.getRaw("e3.am1[2]").toString());
    Assertions.assertEquals("8492292414932401114", event2.getRaw("e3.am1[3]").toString());
    Assertions.assertEquals("-1192952196729165097", event2.getRaw("e4.e4.m4").toString());
  }
}
