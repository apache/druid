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
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.groups.Default;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BytesTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testNumberString()
  {
    Assert.assertEquals(0, Bytes.parse("0"));
    Assert.assertEquals(1, Bytes.parse("1"));
    Assert.assertEquals(10000000, Bytes.parse("10000000"));
  }

  @Test
  public void testWithWhiteSpace()
  {
    Assert.assertEquals(12345, Bytes.parse(" 12345 "));
    Assert.assertEquals(12345, Bytes.parse("\t12345\t"));
  }

  @Test
  public void testK()
  {
    Assert.assertEquals(1000, Bytes.parse("1k"));
    Assert.assertEquals(1000, Bytes.parse("1K"));
  }

  @Test
  public void testM()
  {
    Assert.assertEquals(1000_000, Bytes.parse("1m"));
    Assert.assertEquals(1000_000, Bytes.parse("1M"));
  }

  @Test
  public void testG()
  {
    Assert.assertEquals(1000_000_000, Bytes.parse("1g"));
    Assert.assertEquals(1000_000_000, Bytes.parse("1G"));
  }

  @Test
  public void testT()
  {
    Assert.assertEquals(1000_000_000_000L, Bytes.parse("1t"));
    Assert.assertEquals(1000_000_000_000L, Bytes.parse("1T"));
  }

  @Test
  public void testKiB()
  {
    Assert.assertEquals(1024, Bytes.parse("1kib"));
    Assert.assertEquals(9 * 1024, Bytes.parse("9KiB"));
    Assert.assertEquals(9 * 1024, Bytes.parse("9Kib"));
  }

  @Test
  public void testMiB()
  {
    Assert.assertEquals(1024 * 1024, Bytes.parse("1mib"));
    Assert.assertEquals(9 * 1024 * 1024, Bytes.parse("9MiB"));
    Assert.assertEquals(9 * 1024 * 1024, Bytes.parse("9Mib"));
  }

  @Test
  public void testGiB()
  {
    Assert.assertEquals(1024 * 1024 * 1024, Bytes.parse("1gib"));
    Assert.assertEquals(1024 * 1024 * 1024, Bytes.parse("1GiB"));
    Assert.assertEquals(9L * 1024 * 1024 * 1024, Bytes.parse("9Gib"));
  }

  @Test
  public void testTiB()
  {
    Assert.assertEquals(1024L * 1024 * 1024 * 1024, Bytes.parse("1tib"));
    Assert.assertEquals(9L * 1024 * 1024 * 1024 * 1024, Bytes.parse("9TiB"));
    Assert.assertEquals(9L * 1024 * 1024 * 1024 * 1024, Bytes.parse("9Tib"));
  }

  @Test
  public void testPiB()
  {
    Assert.assertEquals(1024L * 1024 * 1024 * 1024 * 1024, Bytes.parse("1pib"));
    Assert.assertEquals(9L * 1024 * 1024 * 1024 * 1024 * 1024, Bytes.parse("9PiB"));
    Assert.assertEquals(9L * 1024 * 1024 * 1024 * 1024 * 1024, Bytes.parse("9Pib"));
  }

  @Test
  public void testDefault()
  {
    Assert.assertEquals(-123, Bytes.parse(" ", -123));
    Assert.assertEquals(-456, Bytes.parse(null, -456));
    Assert.assertEquals(-789, Bytes.parse("\t", -789));
  }

  static class ExceptionMatcher implements Matcher
  {
    static ExceptionMatcher INVALIDFORMAT = new ExceptionMatcher("Invalid format of number");
    static ExceptionMatcher OVERFLOW = new ExceptionMatcher("Number overflow");

    private String prefix;

    public ExceptionMatcher(String prefix)
    {
      this.prefix = prefix;
    }

    @Override
    public boolean matches(Object item)
    {
      if (!(item instanceof IAE)) {
        return false;
      }

      return ((IAE) item).getMessage().startsWith(prefix);
    }

    @Override
    public void describeMismatch(Object item, Description mismatchDescription)
    {
    }

    @Override
    public void _dont_implement_Matcher___instead_extend_BaseMatcher_()
    {
    }

    @Override
    public void describeTo(Description description)
    {
    }
  }

  @Test
  public void testNull()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    Bytes.parse(null);
  }

  @Test
  public void testEmpty()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    Bytes.parse("");
  }

  @Test
  public void testWhitespace()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    Bytes.parse("   ");
  }

  @Test
  public void testNegative()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    Bytes.parse("-1");
  }

  @Test
  public void testInvalidFormat1()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    Bytes.parse("b");
  }

  @Test
  public void testInvalidFormat2()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    Bytes.parse("B");
  }

  @Test
  public void testInvalidFormat3()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    Bytes.parse("1 b");
  }

  @Test
  public void testInvalidFormat4()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    Bytes.parse("1b");
  }

  @Test
  public void testInvalidFormat5()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    Bytes.parse("tib");
  }

  @Test
  public void testInvalidFormat6()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    Bytes.parse("1 mib");
  }

  @Test
  public void testInvalidFormat7()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    Bytes.parse("gib");
  }

  @Test
  public void testInvalidFormat8()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    Bytes.parse("tib");
  }

  @Test
  public void testInvalidFormat9()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    Bytes.parse(" pib");
  }

  @Test
  public void testInvalidFormat10()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    Bytes.parse("c");
  }

  @Test
  public void testExtraLargeNumber()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    String extraLarge = String.valueOf(Long.MAX_VALUE) + "1";
    Bytes.parse(extraLarge);
  }

  @Test
  public void testOverflowK()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / 1000 + 1) + "k";
    Bytes.parse(overflow);
  }

  @Test
  public void testOverflowM()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / 1000_000 + 1) + "m";
    Bytes.parse(overflow);
  }

  @Test
  public void testOverflowG()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / 1000_000_000L + 1) + "g";
    Bytes.parse(overflow);
  }

  @Test
  public void testOverflowT()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / 1000_000_000_000L + 1) + "t";
    Bytes.parse(overflow);
  }

  @Test
  public void testOverflowP()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / 1_000_000_000_000_000L + 1) + "p";
    Bytes.parse(overflow);
  }

  @Test
  public void testOverflowKiB()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / 1024 + 1) + "KiB";
    Bytes.parse(overflow);
  }

  @Test
  public void testOverflowMiB()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / (1024 * 1024) + 1) + "MiB";
    Bytes.parse(overflow);
  }

  @Test
  public void testOverflowGiB()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / (1024L * 1024 * 1024) + 1) + "GiB";
    Bytes.parse(overflow);
  }

  @Test
  public void testOverflowTiB()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / (1024L * 1024 * 1024 * 1024) + 1) + "TiB";
    Bytes.parse(overflow);
  }

  @Test
  public void testOverflowPiB()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / (1024L * 1024 * 1024 * 1024 * 1024) + 1) + "PiB";
    Bytes.parse(overflow);
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

  static class TestBytesRange
  {
    @BytesRange(min = 0, max = 5)
    Bytes bytes;

    public TestBytesRange(Bytes bytes)
    {
      this.bytes = bytes;
    }
  }

  @Test
  public void testBytesRange()
  {
    long errorCount = validate(new TestBytesRange(Bytes.valueOf(-1)));
    Assert.assertEquals(1, errorCount);

    errorCount = validate(new TestBytesRange(Bytes.valueOf(0)));
    Assert.assertEquals(0, errorCount);

    errorCount = validate(new TestBytesRange(Bytes.valueOf(5)));
    Assert.assertEquals(0, errorCount);

    errorCount = validate(new TestBytesRange(Bytes.valueOf(6)));
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
