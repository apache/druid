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

package org.apache.druid.data.input;

import com.google.common.base.Predicate;
import org.apache.druid.data.input.impl.RetryingInputStream;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;
import org.apache.druid.java.util.common.RetryUtils;

import java.io.IOException;
import java.io.InputStream;

public interface RetryingInputEntity extends InputEntity
{
  @Override
  default InputStream open() throws IOException
  {
    return new RetryingInputStream<>(
        this,
        new RetryingInputEntityOpenFunction(),
        getRetryCondition(),
        RetryUtils.DEFAULT_MAX_TRIES
    );
  }

  /**
   * Directly opens an {@link InputStream} on the input entity.
   */
  default InputStream readFromStart() throws IOException
  {
    return readFrom(0);
  }

  /**
   * Directly opens an {@link InputStream} starting at the given offset on the input entity.
   *
   * @param offset an offset to start reading from. A non-negative integer counting
   *               the number of bytes from the beginning of the entity
   */
  InputStream readFrom(long offset) throws IOException;

  @Override
  Predicate<Throwable> getRetryCondition();

  class RetryingInputEntityOpenFunction implements ObjectOpenFunction<RetryingInputEntity>
  {
    @Override
    public InputStream open(RetryingInputEntity object) throws IOException
    {
      return object.readFromStart();
    }

    @Override
    public InputStream open(RetryingInputEntity object, long start) throws IOException
    {
      return object.readFrom(start);
    }
  }
}
