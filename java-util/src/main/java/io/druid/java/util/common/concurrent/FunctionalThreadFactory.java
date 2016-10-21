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

package io.druid.java.util.common.concurrent;

import com.google.common.base.Function;

import java.util.concurrent.ThreadFactory;

/**
 */
public class FunctionalThreadFactory implements ThreadFactory
{
  private final ThreadFactory delegate;

  public FunctionalThreadFactory(final String name)
  {
    this(
        new ThreadFactory()
        {
          @Override
          public Thread newThread(Runnable runnable)
          {
            return new Thread(runnable, name);
          }
        }
    );
  }

  public FunctionalThreadFactory(ThreadFactory delegate)
  {
    this.delegate = delegate;
  }

  @Override
  public Thread newThread(Runnable runnable)
  {
    return delegate.newThread(runnable);
  }

  public FunctionalThreadFactory transform(Function<ThreadFactory, ThreadFactory> fn)
  {
    return new FunctionalThreadFactory(fn.apply(delegate));
  }

  public FunctionalThreadFactory transformThread(final Function<Thread, Thread> fn)
  {
    return new FunctionalThreadFactory(new ThreadFactory()
    {
      @Override
      public Thread newThread(Runnable runnable)
      {
        return fn.apply(delegate.newThread(runnable));
      }
    });
  }

  public FunctionalThreadFactory daemonize()
  {
    return transformThread(
        new Function<Thread, Thread>()
        {
          @Override
          public Thread apply(Thread input)
          {
            input.setDaemon(true);
            return input;
          }
        }
    );
  }
}
