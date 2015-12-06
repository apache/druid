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

package io.druid.query.extraction;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

public class TimeFormatExtractionFnTest
{

  private static final long[] timestamps = {
      new DateTime("2015-01-01T23:00:00Z").getMillis(),
      new DateTime("2015-01-02T23:00:00Z").getMillis(),
      new DateTime("2015-03-03T23:00:00Z").getMillis(),
      new DateTime("2015-03-04T23:00:00Z").getMillis(),
      new DateTime("2015-05-02T23:00:00Z").getMillis(),
      new DateTime("2015-12-21T23:00:00Z").getMillis()
  };

  @Test(expected = IllegalArgumentException.class)
  public void testIAEForNullPattern() throws Exception
  {
    new TimeFormatExtractionFn(null, null, null);
  }

  @Test
  public void testDayOfWeekExtraction() throws Exception
  {
    TimeFormatExtractionFn fn = new TimeFormatExtractionFn("EEEE", null, null);
    Assert.assertEquals("Thursday",  fn.apply(timestamps[0]));
    Assert.assertEquals("Friday",    fn.apply(timestamps[1]));
    Assert.assertEquals("Tuesday",   fn.apply(timestamps[2]));
    Assert.assertEquals("Wednesday", fn.apply(timestamps[3]));
    Assert.assertEquals("Saturday",  fn.apply(timestamps[4]));
    Assert.assertEquals("Monday",    fn.apply(timestamps[5]));

    testSerde(fn, "EEEE", null, null);
  }

  @Test
  public void testLocalizedExtraction() throws Exception
  {
    TimeFormatExtractionFn fn = new TimeFormatExtractionFn("EEEE", null, "is");
    Assert.assertEquals("fimmtudagur",  fn.apply(timestamps[0]));
    Assert.assertEquals("föstudagur",   fn.apply(timestamps[1]));
    Assert.assertEquals("þriðjudagur",  fn.apply(timestamps[2]));
    Assert.assertEquals("miðvikudagur", fn.apply(timestamps[3]));
    Assert.assertEquals("laugardagur",  fn.apply(timestamps[4]));
    Assert.assertEquals("mánudagur",    fn.apply(timestamps[5]));

    testSerde(fn, "EEEE", null, "is");
  }

  @Test
  public void testTimeZoneExtraction() throws Exception
  {
    TimeFormatExtractionFn fn = new TimeFormatExtractionFn("'In Berlin ist es schon 'EEEE", DateTimeZone.forID("Europe/Berlin"), "de");
    Assert.assertEquals("In Berlin ist es schon Freitag",    fn.apply(timestamps[0]));
    Assert.assertEquals("In Berlin ist es schon Samstag",    fn.apply(timestamps[1]));
    Assert.assertEquals("In Berlin ist es schon Mittwoch",   fn.apply(timestamps[2]));
    Assert.assertEquals("In Berlin ist es schon Donnerstag", fn.apply(timestamps[3]));
    Assert.assertEquals("In Berlin ist es schon Sonntag",    fn.apply(timestamps[4]));
    Assert.assertEquals("In Berlin ist es schon Dienstag",   fn.apply(timestamps[5]));

    testSerde(fn, "'In Berlin ist es schon 'EEEE", DateTimeZone.forID("Europe/Berlin"), "de");
  }

  public void testSerde(TimeFormatExtractionFn fn, String format, DateTimeZone tz, String locale) throws Exception {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final  String json = objectMapper.writeValueAsString(fn);
    TimeFormatExtractionFn deserialized = objectMapper.readValue(json, TimeFormatExtractionFn.class);

    Assert.assertEquals(format, deserialized.getFormat());
    Assert.assertEquals(tz, deserialized.getTimeZone());
    Assert.assertEquals(locale, deserialized.getLocale());

    Assert.assertEquals(fn, deserialized);
  }

  @Test
  public void testSerdeFromJson() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json = "{ \"type\" : \"timeFormat\", \"format\" : \"HH\" }";
    TimeFormatExtractionFn extractionFn = (TimeFormatExtractionFn) objectMapper.readValue(json, ExtractionFn.class);

    Assert.assertEquals("HH", extractionFn.getFormat());
    Assert.assertEquals(null, extractionFn.getLocale());
    Assert.assertEquals(null, extractionFn.getTimeZone());

    // round trip
    Assert.assertEquals(
        extractionFn,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionFn),
            ExtractionFn.class
        )
    );
  }
}
