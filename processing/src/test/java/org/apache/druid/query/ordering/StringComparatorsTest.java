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

package org.apache.druid.query.ordering;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class StringComparatorsTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  
  private void commonTest(StringComparator comparator)
  {
    // equality test
    Assertions.assertTrue(comparator.compare(null, null) == 0);
    Assertions.assertTrue(comparator.compare("", "") == 0);
    Assertions.assertTrue(comparator.compare("123", "123") == 0);
    Assertions.assertTrue(comparator.compare("abc123", "abc123") == 0);
    
    // empty strings < non-empty
    Assertions.assertTrue(comparator.compare("", "abc") < 0);
    Assertions.assertTrue(comparator.compare("abc", "") > 0);
    
    // null first test
    Assertions.assertTrue(comparator.compare(null, "apple") < 0);
  }
  
  @Test
  public void testLexicographicComparator()
  {
    commonTest(StringComparators.LEXICOGRAPHIC);

    Assertions.assertTrue(StringComparators.LEXICOGRAPHIC.compare("apple", "banana") < 0);
    Assertions.assertTrue(StringComparators.LEXICOGRAPHIC.compare("banana", "banana") == 0);
  }

  @Test
  public void testAlphanumericComparator()
  {
    commonTest(StringComparators.ALPHANUMERIC);

    // numbers < non numeric
    Assertions.assertTrue(StringComparators.ALPHANUMERIC.compare("123", "abc") < 0);
    Assertions.assertTrue(StringComparators.ALPHANUMERIC.compare("abc", "123") > 0);

    // numbers ordered numerically
    Assertions.assertTrue(StringComparators.ALPHANUMERIC.compare("2", "11") < 0);
    Assertions.assertTrue(StringComparators.ALPHANUMERIC.compare("a2", "a11") < 0);

    // leading zeros
    Assertions.assertTrue(StringComparators.ALPHANUMERIC.compare("02", "11") < 0);
    Assertions.assertTrue(StringComparators.ALPHANUMERIC.compare("02", "002") < 0);

    // decimal points ...
    Assertions.assertTrue(StringComparators.ALPHANUMERIC.compare("1.3", "1.5") < 0);

    // ... don't work too well
    Assertions.assertTrue(StringComparators.ALPHANUMERIC.compare("1.3", "1.15") < 0);

    // but you can sort ranges
    List<String> sorted = Lists.newArrayList("1-5", "11-15", "16-20", "21-25", "26-30", "6-10", "Other");
    Collections.sort(sorted, StringComparators.ALPHANUMERIC);

    Assertions.assertEquals(
        ImmutableList.of("1-5", "6-10", "11-15", "16-20", "21-25", "26-30", "Other"),
        sorted
    );

    List<String> sortedFixedDecimal = Lists.newArrayList(
        "Other", "[0.00-0.05)", "[0.05-0.10)", "[0.10-0.50)", "[0.50-1.00)",
        "[1.00-5.00)", "[5.00-10.00)", "[10.00-20.00)"
    );

    Collections.sort(sortedFixedDecimal, StringComparators.ALPHANUMERIC);

    Assertions.assertEquals(
        ImmutableList.of(
            "[0.00-0.05)", "[0.05-0.10)", "[0.10-0.50)", "[0.50-1.00)",
            "[1.00-5.00)", "[5.00-10.00)", "[10.00-20.00)", "Other"
        ),
        sortedFixedDecimal
    ); 
  }

  @Test
  public void testStrlenComparator()
  {
    commonTest(StringComparators.STRLEN);

    Assertions.assertTrue(StringComparators.STRLEN.compare("a", "apple") < 0);
    Assertions.assertTrue(StringComparators.STRLEN.compare("a", "elppa") < 0);
    Assertions.assertTrue(StringComparators.STRLEN.compare("apple", "elppa") < 0);
  }

  @Test
  public void testNumericComparator()
  {
    commonTest(StringComparators.NUMERIC);

    Assertions.assertTrue(StringComparators.NUMERIC.compare("-1230.452487532", "6893") < 0);

    List<String> values = Arrays.asList("-1", "-1.10", "-1.2", "-100", "-2", "0", "1", "1.10", "1.2", "2", "100");
    Collections.sort(values, StringComparators.NUMERIC);

    Assertions.assertEquals(
        Arrays.asList("-100", "-2", "-1.2", "-1.10", "-1", "0", "1", "1.10", "1.2", "2", "100"),
        values
    );


    Assertions.assertTrue(StringComparators.NUMERIC.compare(null, null) == 0);
    Assertions.assertTrue(StringComparators.NUMERIC.compare(null, "1001") < 0);
    Assertions.assertTrue(StringComparators.NUMERIC.compare("1001", null) > 0);

    Assertions.assertTrue(StringComparators.NUMERIC.compare("-500000000.14124", "CAN'T TOUCH THIS") > 0);
    Assertions.assertTrue(StringComparators.NUMERIC.compare("CAN'T PARSE THIS", "-500000000.14124") < 0);

    Assertions.assertTrue(StringComparators.NUMERIC.compare("CAN'T PARSE THIS", "CAN'T TOUCH THIS") < 0);
  }

  @Test
  public void testVersionComparator()
  {
    commonTest(StringComparators.VERSION);

    Assertions.assertTrue(StringComparators.VERSION.compare("02", "002") == 0);
    Assertions.assertTrue(StringComparators.VERSION.compare("1.0", "2.0") < 0);
    Assertions.assertTrue(StringComparators.VERSION.compare("9.1", "10.0") < 0);
    Assertions.assertTrue(StringComparators.VERSION.compare("1.1.1", "2.0") < 0);
    Assertions.assertTrue(StringComparators.VERSION.compare("1.0-SNAPSHOT", "1.0") < 0);
    Assertions.assertTrue(StringComparators.VERSION.compare("2.0.1-xyz-1", "2.0.1-1-xyz") < 0);
    Assertions.assertTrue(StringComparators.VERSION.compare("1.0-SNAPSHOT", "1.0-Final") < 0);
  }

  @Test
  public void testNaturalComparator()
  {
    Assertions.assertThrows(DruidException.class, () -> StringComparators.NATURAL.compare("str1", "str2"));
  }

  @Test
  public void testLexicographicComparatorSerdeTest() throws IOException
  {
    String expectJsonSpec = "{\"type\":\"lexicographic\"}";

    String jsonSpec = JSON_MAPPER.writeValueAsString(StringComparators.LEXICOGRAPHIC);
    Assertions.assertEquals(expectJsonSpec, jsonSpec);
    Assertions.assertEquals(StringComparators.LEXICOGRAPHIC, JSON_MAPPER.readValue(expectJsonSpec, StringComparator.class));

    String makeFromJsonSpec = "\"lexicographic\"";
    Assertions.assertEquals(
        StringComparators.LEXICOGRAPHIC,
        JSON_MAPPER.readValue(makeFromJsonSpec, StringComparator.class)
    );
  }
  
  @Test
  public void testAlphanumericComparatorSerdeTest() throws IOException
  {
    String expectJsonSpec = "{\"type\":\"alphanumeric\"}";

    String jsonSpec = JSON_MAPPER.writeValueAsString(StringComparators.ALPHANUMERIC);
    Assertions.assertEquals(expectJsonSpec, jsonSpec);
    Assertions.assertEquals(StringComparators.ALPHANUMERIC, JSON_MAPPER.readValue(expectJsonSpec, StringComparator.class));

    String makeFromJsonSpec = "\"alphanumeric\"";
    Assertions.assertEquals(StringComparators.ALPHANUMERIC, JSON_MAPPER.readValue(makeFromJsonSpec, StringComparator.class));
  }
  
  @Test
  public void testStrlenComparatorSerdeTest() throws IOException
  {
    String expectJsonSpec = "{\"type\":\"strlen\"}";
    
    String jsonSpec = JSON_MAPPER.writeValueAsString(StringComparators.STRLEN);
    Assertions.assertEquals(expectJsonSpec, jsonSpec);
    Assertions.assertEquals(StringComparators.STRLEN, JSON_MAPPER.readValue(expectJsonSpec, StringComparator.class));

    String makeFromJsonSpec = "\"strlen\"";
    Assertions.assertEquals(StringComparators.STRLEN, JSON_MAPPER.readValue(makeFromJsonSpec, StringComparator.class));
  }

  @Test
  public void testNumericComparatorSerdeTest() throws IOException
  {
    String expectJsonSpec = "{\"type\":\"numeric\"}";

    String jsonSpec = JSON_MAPPER.writeValueAsString(StringComparators.NUMERIC);
    Assertions.assertEquals(expectJsonSpec, jsonSpec);
    Assertions.assertEquals(StringComparators.NUMERIC, JSON_MAPPER.readValue(expectJsonSpec, StringComparator.class));

    String makeFromJsonSpec = "\"numeric\"";
    Assertions.assertEquals(StringComparators.NUMERIC, JSON_MAPPER.readValue(makeFromJsonSpec, StringComparator.class));

    makeFromJsonSpec = "\"NuMeRiC\"";
    Assertions.assertEquals(StringComparators.NUMERIC, JSON_MAPPER.readValue(makeFromJsonSpec, StringComparator.class));
  }
  
  @Test
  public void testNaturalComparatorSerdeTest() throws IOException
  {
    String expectJsonSpec = "{\"type\":\"natural\"}";

    String jsonSpec = JSON_MAPPER.writeValueAsString(StringComparators.NATURAL);
    Assertions.assertEquals(expectJsonSpec, jsonSpec);
    Assertions.assertEquals(StringComparators.NATURAL, JSON_MAPPER.readValue(expectJsonSpec, StringComparator.class));

    String makeFromJsonSpec = "\"natural\"";
    Assertions.assertEquals(StringComparators.NATURAL, JSON_MAPPER.readValue(makeFromJsonSpec, StringComparator.class));

    makeFromJsonSpec = "\"NaTuRaL\"";
    Assertions.assertEquals(StringComparators.NATURAL, JSON_MAPPER.readValue(makeFromJsonSpec, StringComparator.class));
  }
}
