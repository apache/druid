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

package org.apache.druid.common;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.StringUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

public class EitherTest
{
  @Test
  public void testValueString()
  {
    final Either<String, String> either = Either.value("yay");

    Assert.assertFalse(either.isError());
    Assert.assertTrue(either.isValue());
    Assert.assertEquals("yay", either.valueOrThrow());

    final IllegalStateException e = Assert.assertThrows(IllegalStateException.class, either::error);
    MatcherAssert.assertThat(e.getMessage(), CoreMatchers.startsWith("Not an error"));

    // Test toString.
    Assert.assertEquals("Value[yay]", either.toString());

    // Test map.
    Assert.assertEquals(Either.value("YAY"), either.map(StringUtils::toUpperCase));
  }

  @Test
  public void testValueNull()
  {
    final Either<String, String> either = Either.value(null);

    Assert.assertFalse(either.isError());
    Assert.assertTrue(either.isValue());
    Assert.assertNull(either.valueOrThrow());

    final IllegalStateException e = Assert.assertThrows(IllegalStateException.class, either::error);
    MatcherAssert.assertThat(e.getMessage(), CoreMatchers.startsWith("Not an error"));

    // Test toString.
    Assert.assertEquals("Value[null]", either.toString());

    // Test map.
    Assert.assertEquals(Either.value("nullxyz"), either.map(s -> s + "xyz"));
  }

  @Test
  public void testErrorString()
  {
    final Either<String, Object> either = Either.error("oh no");

    Assert.assertTrue(either.isError());
    Assert.assertFalse(either.isValue());
    Assert.assertEquals("oh no", either.error());

    final RuntimeException e = Assert.assertThrows(RuntimeException.class, either::valueOrThrow);
    MatcherAssert.assertThat(e.getMessage(), CoreMatchers.equalTo("oh no"));

    // Test toString.
    Assert.assertEquals("Error[oh no]", either.toString());

    // Test map.
    Assert.assertEquals(either, either.map(o -> "this does nothing because the Either is an error"));
  }

  @Test
  public void testErrorThrowable()
  {
    final Either<Throwable, Object> either = Either.error(new AssertionError("oh no"));

    Assert.assertTrue(either.isError());
    Assert.assertFalse(either.isValue());
    MatcherAssert.assertThat(either.error(), CoreMatchers.instanceOf(AssertionError.class));
    MatcherAssert.assertThat(either.error().getMessage(), CoreMatchers.equalTo("oh no"));

    final RuntimeException e = Assert.assertThrows(RuntimeException.class, either::valueOrThrow);
    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(AssertionError.class));
    MatcherAssert.assertThat(e.getCause().getMessage(), CoreMatchers.equalTo("oh no"));

    // Test toString.
    Assert.assertEquals("Error[java.lang.AssertionError: oh no]", either.toString());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(Either.class).usingGetClass().verify();
  }
}
