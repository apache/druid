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

package org.apache.druid.io;

import org.apache.druid.java.util.common.StringUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class LimitedOutputStreamTest
{
  @Test
  public void test_limitZero() throws IOException
  {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
         final OutputStream stream =
             new LimitedOutputStream(baos, 0, LimitedOutputStreamTest::makeErrorMessage)) {
      final IOException e = Assert.assertThrows(
          IOException.class,
          () -> stream.write('b')
      );

      MatcherAssert.assertThat(e, ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo("Limit[0] exceeded")));
    }
  }

  @Test
  public void test_limitThree() throws IOException
  {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
         final OutputStream stream =
             new LimitedOutputStream(baos, 3, LimitedOutputStreamTest::makeErrorMessage)) {
      stream.write('a');
      stream.write(new byte[]{'b'});
      stream.write(new byte[]{'c'}, 0, 1);
      final IOException e = Assert.assertThrows(
          IOException.class,
          () -> stream.write('d')
      );

      MatcherAssert.assertThat(e, ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo("Limit[3] exceeded")));
    }
  }

  private static String makeErrorMessage(final long limit)
  {
    return StringUtils.format("Limit[%d] exceeded", limit);
  }
}
