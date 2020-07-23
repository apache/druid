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
import org.junit.Assert;
import org.junit.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.groups.Default;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BytesTest
{
  @Test
  public void testNumberString()
  {
    Assert.assertEquals(1, Bytes.parse("1", 0L));
    Assert.assertEquals(10000000, Bytes.parse("10000000", 0));
  }

  @Test
  public void testWithWhiteSpace()
  {
    Assert.assertEquals(12345, Bytes.parse(" 12345 ", 0));
    Assert.assertEquals(12345, Bytes.parse("\t12345\t", 0));
  }

  @Test
  public void testK()
  {
    Assert.assertEquals(1000, Bytes.parse("1k", 0));
    Assert.assertEquals(1000, Bytes.parse("1K", 0));
  }

  @Test
  public void testM()
  {
    Assert.assertEquals(1000_000, Bytes.parse("1m", 0));
    Assert.assertEquals(1000_000, Bytes.parse("1M", 0));
  }

  @Test
  public void testG()
  {
    Assert.assertEquals(1000_000_000, Bytes.parse("1g", 0));
    Assert.assertEquals(1000_000_000, Bytes.parse("1G", 0));
  }

  @Test
  public void testT()
  {
    Assert.assertEquals(1000_000_000_000L, Bytes.parse("1t", 0));
    Assert.assertEquals(1000_000_000_000L, Bytes.parse("1T", 0));
  }

  @Test
  public void testKiB()
  {
    Assert.assertEquals(1024, Bytes.parse("1kib", 0));
    Assert.assertEquals(9 * 1024, Bytes.parse("9KiB", 0));
    Assert.assertEquals(9 * 1024, Bytes.parse("9Kib", 0));
  }

  @Test
  public void testMb()
  {
    Assert.assertEquals(1024 * 1024, Bytes.parse("1mib", 0));
    Assert.assertEquals(9 * 1024 * 1024, Bytes.parse("9MiB", 0));
    Assert.assertEquals(9 * 1024 * 1024, Bytes.parse("9Mib", 0));
  }

  @Test
  public void testGiB()
  {
    Assert.assertEquals(1024 * 1024 * 1024, Bytes.parse("1gib", 0));
    Assert.assertEquals(1024 * 1024 * 1024, Bytes.parse("1GiB", 0));
    Assert.assertEquals(9L * 1024 * 1024 * 1024, Bytes.parse("9Gib", 0));
  }

  @Test
  public void testTiB()
  {
    Assert.assertEquals(1024 * 1024 * 1024 * 1024L, Bytes.parse("1tib", 0));
    Assert.assertEquals(9L * 1024 * 1024 * 1024 * 1024, Bytes.parse("9TiB", 0));
    Assert.assertEquals(9L * 1024 * 1024 * 1024 * 1024, Bytes.parse("9Tib", 0));
  }

  @Test
  public void testPiB()
  {
    Assert.assertEquals(1024L * 1024 * 1024 * 1024 * 1024, Bytes.parse("1pib", 0));
    Assert.assertEquals(9L * 1024 * 1024 * 1024 * 1024 * 1024, Bytes.parse("9PiB", 0));
    Assert.assertEquals(9L * 1024 * 1024 * 1024 * 1024 * 1024, Bytes.parse("9Pib", 0));
  }

  @Test
  public void testDefault()
  {
    Assert.assertEquals(-123, Bytes.parse(" ", -123));
    Assert.assertEquals(-456, Bytes.parse(null, -456));
    Assert.assertEquals(-789, Bytes.parse("\t", -789));
  }

  @Test
  public void testException()
  {
    try {
      Bytes.parse("b", 0);
      Assert.assertFalse("IAE should be thrown", true);
    }
    catch (IAE e) {
      Assert.assertTrue(true);
    }

    try {
      Bytes.parse("1 b", 0);
      Assert.assertFalse("IAE should be thrown", true);
    }
    catch (IAE e) {
      Assert.assertTrue(true);
    }

    try {
      Bytes.parse("tb", 0);
      Assert.assertFalse("IAE should be thrown", true);
    }
    catch (IAE e) {
      Assert.assertTrue(true);
    }

    try {
      Bytes.parse("1 mb", 0);
      Assert.assertFalse("IAE should be thrown", true);
    }
    catch (IAE e) {
      Assert.assertTrue(true);
    }

    try {
      Bytes.parse("gb", 0);
      Assert.assertFalse("IAE should be thrown", true);
    }
    catch (IAE e) {
      Assert.assertTrue(true);
    }

    try {
      Bytes.parse("tb", 0);
      Assert.assertFalse("IAE should be thrown", true);
    }
    catch (IAE e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testOverflow()
  {
    try {
      Bytes.parse("123456789123456789123", 0);
      Assert.assertFalse("IAE should be thrown", true);
    }
    catch (IAE e) {
    }

    try {
      String max = (Long.MAX_VALUE / 1000 + 1) + "k";
      Bytes.parse(max, 0);
      Assert.assertFalse("IAE should be thrown", true);
    }
    catch (IAE e) {
    }

    try {
      String max = (Long.MAX_VALUE / 1000_000 + 1) + "m";
      Bytes.parse(max, 0);
      Assert.assertFalse("IAE should be thrown", true);
    }
    catch (IAE e) {
    }

    try {
      String max = (Long.MAX_VALUE / 1000_000_000L + 1) + "g";
      Bytes.parse(max, 0);
      Assert.assertFalse("IAE should be thrown", true);
    }
    catch (IAE e) {
    }

    try {
      String max = (Long.MAX_VALUE / 1000_000_000_000L + 1) + "t";
      Bytes.parse(max, 0);
      Assert.assertFalse("IAE should be thrown", true);
    }
    catch (IAE e) {
    }

    try {
      String max = (Long.MAX_VALUE / 1024 + 1) + "kb";
      Bytes.parse(max, 0);
      Assert.assertFalse("IAE should be thrown", true);
    }
    catch (IAE e) {
    }

    try {
      String max = (Long.MAX_VALUE / (1024 * 1024) + 1) + "mb";
      Bytes.parse(max, 0);
      Assert.assertFalse("IAE should be thrown", true);
    }
    catch (IAE e) {
    }

    try {
      String max = (Long.MAX_VALUE / (1024L * 1024 * 1024) + 1) + "gb";
      Bytes.parse(max, 0);
      Assert.assertFalse("IAE should be thrown", true);
    }
    catch (IAE e) {
    }

    try {
      String max = (Long.MAX_VALUE / (1024L * 1024 * 1024 * 1024) + 1) + "tb";
      Bytes.parse(max, 0);
      Assert.assertFalse("IAE should be thrown", true);
    }
    catch (IAE e) {
    }
  }

  @Test
  public void testJSON() throws JsonProcessingException
  {
    ObjectMapper mapper = new ObjectMapper();
    Bytes bytes = new Bytes("5m");
    String serialized = mapper.writeValueAsString(bytes);
    Bytes deserialized = mapper.readValue(serialized, Bytes.class);
    Assert.assertEquals(bytes, deserialized);
  }

  public static class BytesRangeTest
  {
    @BytesRange(min = 0, max = 5)
    Bytes bytes;

    public BytesRangeTest(Bytes bytes)
    {
      this.bytes = bytes;
    }
  }

  @Test
  public void testValidator()
  {
    long errorCount = validate(new BytesRangeTest(Bytes.valueOf(-1)));
    Assert.assertEquals(1, errorCount);

    errorCount = validate(new BytesRangeTest(Bytes.valueOf(0)));
    Assert.assertEquals(0, errorCount);

    errorCount = validate(new BytesRangeTest(Bytes.valueOf(5)));
    Assert.assertEquals(0, errorCount);

    errorCount = validate(new BytesRangeTest(Bytes.valueOf(6)));
    Assert.assertEquals(1, errorCount);
  }

  private static <T> long validate(T obj)
  {
    Validator validator = Validation.buildDefaultValidatorFactory()
                                    .getValidator();

    Map<String, StringBuilder> errorMap = new HashMap<>();
    Set<ConstraintViolation<T>> set = validator.validate(obj, Default.class);
    return set == null ? 0 : set.size();
  }
}
