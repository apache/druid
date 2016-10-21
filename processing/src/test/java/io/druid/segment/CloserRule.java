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

package io.druid.segment;

import com.google.common.io.Closer;
import io.druid.java.util.common.logger.Logger;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.Closeable;
import java.io.IOException;

public class CloserRule implements TestRule
{
  private final boolean throwException;

  public CloserRule(boolean throwException)
  {
    this.throwException = throwException;
  }

  private static final Logger LOG = new Logger(CloserRule.class);
  private final Closer closer = Closer.create();

  @Override
  public Statement apply(
      final Statement base, Description description
  )
  {
    return new Statement()
    {
      @Override
      public void evaluate() throws Throwable
      {
        try {
          base.evaluate();
        }
        catch (Throwable e) {
          throw closer.rethrow(e);
        }
        finally {
          closer.close();
        }
      }
    };
  }

  public <T extends Closeable> T closeLater(final T closeable)
  {
    closer.register(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        if (throwException) {
          closeable.close();
        } else {
          try {
            closeable.close();
          }
          catch (IOException e) {
            LOG.warn(e, "Error closing [%s]", closeable);
          }
        }
      }
    });
    return closeable;
  }
}
