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

package io.druid.java.util.common;

import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class StreamUtilsTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Test
  public void testRetryExceptionOnFlush()
  {
    final byte[] bytes = new byte[1 << 10];
    Random random = new Random(47831947819L);
    random.nextBytes(bytes);
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final AtomicLong outputFlushes = new AtomicLong(0);
    Assert.assertEquals(
        bytes.length,
        StreamUtils.retryCopy(
            new ByteSource()
            {
              @Override
              public InputStream openStream() throws IOException
              {
                return new ByteArrayInputStream(bytes);
              }
            },
            new ByteSink()
            {
              @Override
              public OutputStream openStream() throws IOException
              {
                byteArrayOutputStream.reset();
                return new FilterOutputStream(byteArrayOutputStream)
                {
                  @Override
                  public void flush() throws IOException
                  {
                    if (outputFlushes.getAndIncrement() > 0) {
                      out.flush();
                    } else {
                      throw new IOException("Test exception");
                    }
                  }
                };
              }
            },
            FileUtils.IS_EXCEPTION,
            10
        )
    );
    Assert.assertEquals(4, outputFlushes.get());// 2 closes and 2 manual flushes
    Assert.assertArrayEquals(bytes, byteArrayOutputStream.toByteArray());
  }
}
