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

package org.apache.druid.java.util.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class HumanReadableDurationTest
{
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @ParameterizedTest
  @CsvSource({
      "0, 0",
      "30000, 30000",
      "500ms, 500",
      "30s, 30000",
      "5m, 300000",
      "2h, 7200000",
      "7d, 604800000",
      "250us, 1",
      "1500000ns, 2",
      "'  2h  ', 7200000",
      "PT30S, 30000",
      "PT1.5S, 1500",
      "P1DT12H, 129600000"
  })
  public void testValidDurations(String text, long expectedMillis)
  {
    Assertions.assertEquals(expectedMillis, HumanReadableDuration.parse(text).getMillis());
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "", " ", "-5s", "1.5h", "2h45m", "30 s", "5 m",
      "30S", "5M", "2H", "5min", "3sec", "1w", "P1M", "P1Y",
      "9999999999d", "s", "-1", "9223372036854775807"
  })
  public void testInvalidDurations(String text)
  {
    Assertions.assertThrows(IAE.class, () -> HumanReadableDuration.parse(text));
  }

  @Test
  public void testNullIsRejected()
  {
    Assertions.assertThrows(IAE.class, () -> HumanReadableDuration.parse(null));
  }

  @Test
  public void testIsoWeekDurationCompatibility()
  {
    Assertions.assertEquals(0L, HumanReadableDuration.parse("P0W").getMillis());
    Assertions.assertEquals(604_800_000L, HumanReadableDuration.parse("P1W").getMillis());
    Assertions.assertEquals(1_209_600_000L, HumanReadableDuration.parse("P2W").getMillis());
    Assertions.assertEquals(777_600_000L, HumanReadableDuration.parse("P1W2D").getMillis());
    Assertions.assertEquals(615_600_000L, HumanReadableDuration.parse("P1WT3H").getMillis());
    Assertions.assertEquals(788_400_000L, HumanReadableDuration.parse("P1W2DT3H").getMillis());

    Assertions.assertThrows(IAE.class, () -> HumanReadableDuration.parse("P-1W"));
    Assertions.assertThrows(IAE.class, () -> HumanReadableDuration.parse("P20000W"));
  }
  @Test
  public void testLegacySeconds()
  {
    Assertions.assertEquals(
        30_000,
        HumanReadableDuration.parse("30", TimeUnit.SECONDS).getMillis()
    );
    Assertions.assertEquals(
        30_000,
        HumanReadableDuration.parse("30s", TimeUnit.SECONDS).getMillis()
    );
  }

  @Test
  public void testNanosecondPrecisionAndCeiling()
  {
    Assertions.assertEquals(1, HumanReadableDuration.ofNanos(1).getMillis());
    Assertions.assertEquals(1, HumanReadableDuration.ofNanos(1_000_000).getMillis());
    Assertions.assertEquals(2, HumanReadableDuration.ofNanos(1_000_001).getMillis());
    Assertions.assertEquals(Duration.ofNanos(250_000), HumanReadableDuration.parse("250us").toDuration());
  }

  @Test
  public void testOverflowAndNegativeValues()
  {
    Assertions.assertThrows(IAE.class, () -> HumanReadableDuration.ofMillis(Long.MAX_VALUE));
    Assertions.assertThrows(IAE.class, () -> HumanReadableDuration.ofMillis(-1));
    Assertions.assertThrows(IAE.class, () -> HumanReadableDuration.ofNanos(-1));
  }

  @Test
  public void testEqualityAndHashCode()
  {
    final HumanReadableDuration a = HumanReadableDuration.parse("30s");
    final HumanReadableDuration b = HumanReadableDuration.parse("30000ms");
    Assertions.assertEquals(a, b);
    Assertions.assertEquals(a.hashCode(), b.hashCode());
    Assertions.assertNotEquals(a, HumanReadableDuration.parse("31s"));
  }

  @Test
  public void testJsonBackwardCompatibility() throws JsonProcessingException
  {
    Assertions.assertEquals(
        HumanReadableDuration.parse("30s"),
        MAPPER.readValue("30000", HumanReadableDuration.class)
    );
    Assertions.assertEquals(
        HumanReadableDuration.parse("30s"),
        MAPPER.readValue("\"PT30S\"", HumanReadableDuration.class)
    );
    Assertions.assertEquals("30000", MAPPER.writeValueAsString(HumanReadableDuration.parse("30s")));
    Assertions.assertEquals("\"250us\"", MAPPER.writeValueAsString(HumanReadableDuration.parse("250us")));
  }

  @Test
  public void testJsonRejectsInvalidValues()
  {
    Assertions.assertThrows(
        JsonProcessingException.class,
        () -> MAPPER.readValue("\"P1M\"", HumanReadableDuration.class)
    );
    Assertions.assertThrows(
        JsonProcessingException.class,
        () -> MAPPER.readValue("-1", HumanReadableDuration.class)
    );
  }
}
