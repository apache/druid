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

import com.metamx.common.logger.Logger;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class CloserRule implements TestRule
{
  private final boolean throwException;

  public CloserRule(boolean throwException)
  {
    this.throwException = throwException;
  }

  private static final Logger log = new Logger(CloserRule.class);
  private final List<AutoCloseable> autoCloseables = new LinkedList<>();

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
        Throwable baseThrown = null;
        try {
          base.evaluate();
        }
        catch (Throwable e) {
          baseThrown = e;
        }
        finally {
          Throwable exception = null;
          for (AutoCloseable autoCloseable : autoCloseables) {
            try {
              autoCloseable.close();
            }
            catch (Exception e) {
              exception = suppressOrSet(exception, e);
            }
          }
          autoCloseables.clear();
          if (exception != null) {
            if (throwException && baseThrown == null) {
              throw exception;
            } else if (baseThrown != null) {
              baseThrown.addSuppressed(exception);
            } else {
              log.error(exception, "Exception closing resources");
            }
          }
          if (baseThrown != null) {
            throw baseThrown;
          }
        }
      }
    };
  }

  private static Throwable suppressOrSet(Throwable prior, Throwable other)
  {
    if (prior == null) {
      prior = new IOException("Error closing resources");
    }
    prior.addSuppressed(other);
    return prior;
  }

  public <T extends AutoCloseable> T closeLater(T autoCloseable)
  {
    autoCloseables.add(autoCloseable);
    return autoCloseable;
  }
}
