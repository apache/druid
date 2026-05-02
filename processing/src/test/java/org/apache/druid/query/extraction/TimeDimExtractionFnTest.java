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

package org.apache.druid.query.extraction;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 */
public class TimeDimExtractionFnTest
{
  private static final String[] DIMS = {
      "01/01/2012",
      "01/02/2012",
      "03/03/2012",
      "03/04/2012",
      "05/05/2012",
      "12/21/2012"
  };

  @Test
  public void testEmptyNullAndUnparseableExtraction()
  {
    for (boolean joda : Arrays.asList(true, false)) {
      ExtractionFn extractionFn = new TimeDimExtractionFn("MM/dd/yyyy", "MM/yyyy", joda);

      Assertions.assertNull(extractionFn.apply(null));
      Assertions.assertEquals("", extractionFn.apply(""));
      Assertions.assertEquals("foo", extractionFn.apply("foo"));
    }
  }

  @Test
  public void testMonthExtraction()
  {
    Set<String> months = new HashSet<>();
    ExtractionFn extractionFn = new TimeDimExtractionFn("MM/dd/yyyy", "MM/yyyy", false);

    for (String dim : DIMS) {
      months.add(extractionFn.apply(dim));
    }

    Assertions.assertEquals(4, months.size());
    Assertions.assertTrue(months.contains("01/2012"));
    Assertions.assertTrue(months.contains("03/2012"));
    Assertions.assertTrue(months.contains("05/2012"));
    Assertions.assertTrue(months.contains("12/2012"));
  }

  @Test
  public void testMonthExtractionJoda()
  {
    Set<String> months = new HashSet<>();
    ExtractionFn extractionFn = new TimeDimExtractionFn("MM/dd/yyyy", "MM/yyyy", true);

    for (String dim : DIMS) {
      months.add(extractionFn.apply(dim));
    }

    Assertions.assertEquals(4, months.size());
    Assertions.assertTrue(months.contains("01/2012"));
    Assertions.assertTrue(months.contains("03/2012"));
    Assertions.assertTrue(months.contains("05/2012"));
    Assertions.assertTrue(months.contains("12/2012"));
  }

  @Test
  public void testQuarterExtraction()
  {
    Set<String> quarters = new HashSet<>();
    ExtractionFn extractionFn = new TimeDimExtractionFn("MM/dd/yyyy", "QQQ/yyyy", false);

    for (String dim : DIMS) {
      quarters.add(extractionFn.apply(dim));
    }

    Assertions.assertEquals(3, quarters.size());
    Assertions.assertTrue(quarters.contains("Q1/2012"));
    Assertions.assertTrue(quarters.contains("Q2/2012"));
    Assertions.assertTrue(quarters.contains("Q4/2012"));
  }

  @Test
  public void testWeeks()
  {
    final TimeDimExtractionFn weekFn = new TimeDimExtractionFn("yyyy-MM-dd", "YYYY-ww", false);
    Assertions.assertEquals("2016-01", weekFn.apply("2015-12-31"));
    Assertions.assertEquals("2016-01", weekFn.apply("2016-01-01"));
    Assertions.assertEquals("2017-01", weekFn.apply("2017-01-01"));
    Assertions.assertEquals("2018-01", weekFn.apply("2017-12-31"));
    Assertions.assertEquals("2018-01", weekFn.apply("2018-01-01"));
  }

  @Test
  public void testWeeksJoda()
  {
    final TimeDimExtractionFn weekFn = new TimeDimExtractionFn("yyyy-MM-dd", "xxxx-ww", true);
    Assertions.assertEquals("2015-53", weekFn.apply("2015-12-31"));
    Assertions.assertEquals("2015-53", weekFn.apply("2016-01-01"));
    Assertions.assertEquals("2016-52", weekFn.apply("2017-01-01"));
    Assertions.assertEquals("2017-52", weekFn.apply("2017-12-31"));
    Assertions.assertEquals("2018-01", weekFn.apply("2018-01-01"));
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json = "{ \"type\" : \"time\", \"timeFormat\" : \"MM/dd/yyyy\", \"resultFormat\" : \"yyyy-MM-dd\", \"joda\" : true }";
    TimeDimExtractionFn extractionFn = (TimeDimExtractionFn) objectMapper.readValue(json, ExtractionFn.class);

    Assertions.assertEquals("MM/dd/yyyy", extractionFn.getTimeFormat());
    Assertions.assertEquals("yyyy-MM-dd", extractionFn.getResultFormat());

    // round trip
    Assertions.assertEquals(
        extractionFn,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionFn),
            ExtractionFn.class
        )
    );
  }
}
