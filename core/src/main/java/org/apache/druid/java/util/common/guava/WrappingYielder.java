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

package org.apache.druid.java.util.common.guava;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;

import java.io.IOException;

final class WrappingYielder<OutType> implements Yielder<OutType>
{
  private final Yielder<OutType> baseYielder;
  private final SequenceWrapper wrapper;

  WrappingYielder(Yielder<OutType> baseYielder, SequenceWrapper wrapper)
  {
    this.baseYielder = baseYielder;
    this.wrapper = wrapper;
  }

  @Override
  public OutType get()
  {
    return baseYielder.get();
  }

  @Override
  public Yielder<OutType> next(final OutType initValue)
  {
    try {
      return wrapper.wrap(new Supplier<Yielder<OutType>>()
      {
        @Override
        public Yielder<OutType> get()
        {
          return new WrappingYielder<>(baseYielder.next(initValue), wrapper);
        }
      });
    }
    catch (Throwable t) {
      // Close on failure
      try {
        wrapper.after(false, t);
      }
      catch (Exception e) {
        t.addSuppressed(e);
      }
      Throwables.propagateIfPossible(t);
      throw new RuntimeException(t);
    }
  }

  @Override
  public boolean isDone()
  {
    return baseYielder.isDone();
  }

  @Override
  public void close() throws IOException
  {
    boolean isDone;
    try {
      isDone = isDone();
      baseYielder.close();
    }
    catch (Throwable t) {
      // Close on failure
      try {
        wrapper.after(false, t);
      }
      catch (Exception e) {
        t.addSuppressed(e);
      }
      Throwables.propagateIfInstanceOf(t, IOException.class);
      Throwables.propagateIfPossible(t);
      throw new RuntimeException(t);
    }
    // "Normal" close
    try {
      wrapper.after(isDone, null);
    }
    catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      Throwables.propagateIfPossible(e);
      throw new RuntimeException(e);
    }
  }
}
