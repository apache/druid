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

package io.druid.server;

import com.google.common.collect.ImmutableMap;
import com.metamx.common.guava.Yielder;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class QueryResourceV3Test
{
  @Test
  public void testYielderCloseOnSerializationSuccess() throws Exception
  {
    final AtomicBoolean isClosed = new AtomicBoolean(false);

    Yielder yielder = new Yielder()
    {
      @Override
      public Object get()
      {
        throw new RuntimeException();
      }

      @Override
      public Yielder next(Object initValue)
      {
        throw new RuntimeException();
      }

      @Override
      public boolean isDone()
      {
        return true;
      }

      @Override
      public void close() throws IOException
      {
        isClosed.set(true);
      }
    };

    Map<String, Object> response = ImmutableMap.of(
        "result", yielder,
        "context", ImmutableMap.of()
    );

    try {
      QueryResourceV3.serializeResponseToOS(
          TestHelper.getObjectMapper().writer(),
          new ByteArrayOutputStream(),
          response
      );
    } catch (Exception ex) {
      Assert.fail();
    }

    Assert.assertTrue(isClosed.get());
  }

  @Test
  public void testYielderCloseOnSerializationFailure() throws Exception
  {
    final AtomicBoolean isClosed = new AtomicBoolean(false);

    Yielder yielder = new Yielder()
    {
      @Override
      public Object get()
      {
        throw new RuntimeException();
      }

      @Override
      public Yielder next(Object initValue)
      {
        throw new RuntimeException();
      }

      @Override
      public boolean isDone()
      {
        throw new RuntimeException();
      }

      @Override
      public void close() throws IOException
      {
        isClosed.set(true);
      }
    };

    Map<String, Object> response = ImmutableMap.of(
        "result", yielder,
        "context", ImmutableMap.of()
    );

    try {
      QueryResourceV3.serializeResponseToOS(
          TestHelper.getObjectMapper().writer(),
          new ByteArrayOutputStream(),
          response
      );
      Assert.fail();
    } catch (Exception ex) {
    }

    Assert.assertTrue(isClosed.get());
  }

  @Test
  public void testYielderCloseOnOutputStreamFailure() throws Exception
  {
    final AtomicBoolean isClosed = new AtomicBoolean(false);

    Yielder yielder = new Yielder()
    {
      @Override
      public Object get()
      {
        throw new RuntimeException();
      }

      @Override
      public Yielder next(Object initValue)
      {
        throw new RuntimeException();
      }

      @Override
      public boolean isDone()
      {
        return true;
      }

      @Override
      public void close() throws IOException
      {
        isClosed.set(true);
      }
    };

    Map<String, Object> response = ImmutableMap.of(
        "result", yielder,
        "context", ImmutableMap.of()
    );

    try {
      QueryResourceV3.serializeResponseToOS(
          TestHelper.getObjectMapper().writer(),
          new OutputStream()
          {
            @Override
            public void write(int b) throws IOException
            {
              throw new RuntimeException("write failed.");
            }
          },
          response
      );

      Assert.fail();
    } catch (Exception ex) {
    }

    Assert.assertTrue(isClosed.get());
  }
}
