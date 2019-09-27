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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

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

      Assert.assertNull(extractionFn.apply(null));
      if (NullHandling.replaceWithDefault()) {
        Assert.assertNull(extractionFn.apply(""));
      } else {
        Assert.assertEquals("", extractionFn.apply(""));
      }
      Assert.assertEquals("foo", extractionFn.apply("foo"));
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

    Assert.assertEquals(months.size(), 4);
    Assert.assertTrue(months.contains("01/2012"));
    Assert.assertTrue(months.contains("03/2012"));
    Assert.assertTrue(months.contains("05/2012"));
    Assert.assertTrue(months.contains("12/2012"));
  }

  @Test
  public void testMonthExtractionJoda()
  {
    Set<String> months = new HashSet<>();
    ExtractionFn extractionFn = new TimeDimExtractionFn("MM/dd/yyyy", "MM/yyyy", true);

    for (String dim : DIMS) {
      months.add(extractionFn.apply(dim));
    }

    Assert.assertEquals(months.size(), 4);
    Assert.assertTrue(months.contains("01/2012"));
    Assert.assertTrue(months.contains("03/2012"));
    Assert.assertTrue(months.contains("05/2012"));
    Assert.assertTrue(months.contains("12/2012"));
  }

  @Test
  public void testQuarterExtraction()
  {
    Set<String> quarters = new HashSet<>();
    ExtractionFn extractionFn = new TimeDimExtractionFn("MM/dd/yyyy", "QQQ/yyyy", false);

    for (String dim : DIMS) {
      quarters.add(extractionFn.apply(dim));
    }

    Assert.assertEquals(quarters.size(), 3);
    Assert.assertTrue(quarters.contains("Q1/2012"));
    Assert.assertTrue(quarters.contains("Q2/2012"));
    Assert.assertTrue(quarters.contains("Q4/2012"));
  }

  @Test
  public void testWeeks()
  {
    final TimeDimExtractionFn weekFn = new TimeDimExtractionFn("yyyy-MM-dd", "YYYY-ww", false);
    Assert.assertEquals("2016-01", weekFn.apply("2015-12-31"));
    Assert.assertEquals("2016-01", weekFn.apply("2016-01-01"));
    Assert.assertEquals("2017-01", weekFn.apply("2017-01-01"));
    Assert.assertEquals("2018-01", weekFn.apply("2017-12-31"));
    Assert.assertEquals("2018-01", weekFn.apply("2018-01-01"));
  }

  @Test
  public void testWeeksJoda()
  {
    final TimeDimExtractionFn weekFn = new TimeDimExtractionFn("yyyy-MM-dd", "xxxx-ww", true);
    Assert.assertEquals("2015-53", weekFn.apply("2015-12-31"));
    Assert.assertEquals("2015-53", weekFn.apply("2016-01-01"));
    Assert.assertEquals("2016-52", weekFn.apply("2017-01-01"));
    Assert.assertEquals("2017-52", weekFn.apply("2017-12-31"));
    Assert.assertEquals("2018-01", weekFn.apply("2018-01-01"));
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json = "{ \"type\" : \"time\", \"timeFormat\" : \"MM/dd/yyyy\", \"resultFormat\" : \"yyyy-MM-dd\", \"joda\" : true }";
    TimeDimExtractionFn extractionFn = (TimeDimExtractionFn) objectMapper.readValue(json, ExtractionFn.class);

    Assert.assertEquals("MM/dd/yyyy", extractionFn.getTimeFormat());
    Assert.assertEquals("yyyy-MM-dd", extractionFn.getResultFormat());

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
