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

package io.druid.benchmark;

import org.junit.Assert;
import org.junit.Test;

import io.druid.java.util.common.parsers.Parser;

import java.util.Map;

public class FlattenJSONBenchmarkUtilTest
{
  @Test
  public void testOne() throws Exception
  {
    FlattenJSONBenchmarkUtil eventGen = new FlattenJSONBenchmarkUtil();

    String newEvent = eventGen.generateFlatEvent();
    String newEvent2 = eventGen.generateNestedEvent();

    Parser flatParser = eventGen.getFlatParser();
    Parser nestedParser = eventGen.getNestedParser();

    Map<String, Object> event = flatParser.parse(newEvent);
    Map<String, Object> event2 = nestedParser.parse(newEvent2);

    checkEvent1(event);
    checkEvent2(event2);
  }

  public void checkEvent1(Map<String, Object> event)
  {
    Assert.assertEquals("2015-09-12T12:10:53.155Z", event.get("ts").toString());
    Assert.assertEquals("-1170723877", event.get("d1").toString());
    Assert.assertEquals("238976084", event.get("d2").toString());
    Assert.assertEquals("0.9818780016507468", event.get("m3").toString());
    Assert.assertEquals("-3.8218837693501747E18", event.get("m4").toString());
    Assert.assertEquals("-509091100", event.get("e1.d1").toString());
    Assert.assertEquals("274706327", event.get("e1.d2").toString());
    Assert.assertEquals("870378185", event.get("e2.d3").toString());
    Assert.assertEquals("-377775321", event.get("e2.d4").toString());
    Assert.assertEquals("-1797988763", event.get("e2.d5").toString());
    Assert.assertEquals("1309474524", event.get("e2.d6").toString());
    Assert.assertEquals("129047958", event.get("e2.ad1[0]").toString());
    Assert.assertEquals("1658972185", event.get("e2.ad1[1]").toString());
    Assert.assertEquals("-997010830", event.get("e2.ad1[2]").toString());
    Assert.assertEquals("-5.8772014847368817E18", event.get("e3.m1").toString());
    Assert.assertEquals("0.4375433369079904", event.get("e3.m2").toString());
    Assert.assertEquals("0.8510482953607659", event.get("e3.m3").toString());
    Assert.assertEquals("-2.3832626488759337E18", event.get("e3.m4").toString());
    Assert.assertEquals("7.9789762132607068E18", event.get("e3.am1[0]").toString());
    Assert.assertEquals("-7.8634787235005573E18", event.get("e3.am1[1]").toString());
    Assert.assertEquals("8.7372945568982446E18", event.get("e3.am1[2]").toString());
    Assert.assertEquals("3.1928124802414899E18", event.get("e3.am1[3]").toString());
    Assert.assertEquals("-3.9806631713718011E18", event.get("e4.e4.m4").toString());
    Assert.assertEquals("-1915243040", event.get("ae1[0].d1").toString());
    Assert.assertEquals("-2020543641", event.get("ae1[1].d1").toString());
    Assert.assertEquals("1414285347", event.get("ae1[2].e1.d2").toString());
  }

  public void checkEvent2(Map<String, Object> event2)
  {
    Assert.assertEquals("728062074", event2.get("ae1[0].d1").toString());
    Assert.assertEquals("1701675101", event2.get("ae1[1].d1").toString());
    Assert.assertEquals("1887775139", event2.get("ae1[2].e1.d2").toString());
    Assert.assertEquals("1375814994", event2.get("e1.d1").toString());
    Assert.assertEquals("-1747933975", event2.get("e1.d2").toString());
    Assert.assertEquals("1616761116", event2.get("e2.ad1[0]").toString());
    Assert.assertEquals("7645432", event2.get("e2.ad1[1]").toString());
    Assert.assertEquals("679897970", event2.get("e2.ad1[2]").toString());
    Assert.assertEquals("-1797792200", event2.get("e2.d3").toString());
    Assert.assertEquals("142582995", event2.get("e2.d4").toString());
    Assert.assertEquals("-1341994709", event2.get("e2.d5").toString());
    Assert.assertEquals("-889954295", event2.get("e2.d6").toString());
    Assert.assertEquals("678995794", event2.get("d1").toString());
    Assert.assertEquals("-1744549866", event2.get("d2").toString());
    Assert.assertEquals("2015-09-12T12:10:53.155Z", event2.get("ts").toString());
    Assert.assertEquals("0.7279915615037622", event2.get("m3").toString());
    Assert.assertEquals("977083178034247050", event2.get("m4").toString());
    Assert.assertEquals("1940993614184952155", event2.get("e3.m1").toString());
    Assert.assertEquals("0.55936084127688", event2.get("e3.m2").toString());
    Assert.assertEquals("0.22821798320943232", event2.get("e3.m3").toString());
    Assert.assertEquals("8176144126231114468", event2.get("e3.m4").toString());
    Assert.assertEquals("-7405674050450245158", event2.get("e3.am1[0]").toString());
    Assert.assertEquals("150970357863018887", event2.get("e3.am1[1]").toString());
    Assert.assertEquals("3261802881806411610", event2.get("e3.am1[2]").toString());
    Assert.assertEquals("8492292414932401114", event2.get("e3.am1[3]").toString());
    Assert.assertEquals("-1192952196729165097", event2.get("e4.e4.m4").toString());
  }
}
