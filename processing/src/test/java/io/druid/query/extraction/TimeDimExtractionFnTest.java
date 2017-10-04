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
import com.google.common.collect.Sets;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 */
public class TimeDimExtractionFnTest
{
  private static final String[] dims = {
      "01/01/2012",
      "01/02/2012",
      "03/03/2012",
      "03/04/2012",
      "05/05/2012",
      "12/21/2012"
  };

  @Test
  public void testEmptyAndNullExtraction()
  {
    Set<String> testPeriod = Sets.newHashSet();
    ExtractionFn extractionFn = new TimeDimExtractionFn("MM/dd/yyyy", "MM/yyyy");

    Assert.assertNull(extractionFn.apply(null));
    Assert.assertNull(extractionFn.apply(""));
  }

  @Test
  public void testMonthExtraction()
  {
    Set<String> months = Sets.newHashSet();
    ExtractionFn extractionFn = new TimeDimExtractionFn("MM/dd/yyyy", "MM/yyyy");

    for (String dim : dims) {
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
    Set<String> quarters = Sets.newHashSet();
    ExtractionFn extractionFn = new TimeDimExtractionFn("MM/dd/yyyy", "QQQ/yyyy");

    for (String dim : dims) {
      quarters.add(extractionFn.apply(dim));
    }

    Assert.assertEquals(quarters.size(), 3);
    Assert.assertTrue(quarters.contains("Q1/2012"));
    Assert.assertTrue(quarters.contains("Q2/2012"));
    Assert.assertTrue(quarters.contains("Q4/2012"));
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json = "{ \"type\" : \"time\", \"timeFormat\" : \"MM/dd/yyyy\", \"resultFormat\" : \"QQQ/yyyy\" }";
    TimeDimExtractionFn extractionFn = (TimeDimExtractionFn) objectMapper.readValue(json, ExtractionFn.class);

    Assert.assertEquals("MM/dd/yyyy", extractionFn.getTimeFormat());
    Assert.assertEquals("QQQ/yyyy", extractionFn.getResultFormat());

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
