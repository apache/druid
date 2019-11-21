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

package org.apache.druid.data.input.impl;

import com.google.common.base.Predicate;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class RetryingInputEntity implements InputEntity
{
  private final InputEntity delegate;
  private final InputEntityOpenFunction openFunction;

  public RetryingInputEntity(InputEntity delegate)
  {
    this.delegate = delegate;
    this.openFunction = new InputEntityOpenFunction();
  }

  @Nullable
  @Override
  public URI getUri()
  {
    return delegate.getUri();
  }

  @Override
  public InputStream open(long offset) throws IOException
  {
    return new RetryingInputStream<>(
        delegate,
        openFunction,
        getRetryCondition(),
        10
    );
  }

  @Override
  public Predicate<Throwable> getRetryCondition()
  {
    return delegate.getRetryCondition();
  }

  private static class InputEntityOpenFunction implements ObjectOpenFunction<InputEntity>
  {
    @Override
    public InputStream open(InputEntity object) throws IOException
    {
      return object.open();
    }

    @Override
    public InputStream open(InputEntity object, long start) throws IOException
    {
      return object.open(start);
    }
  }
}
