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

package io.druid.java.util.common.guava;

import java.io.Closeable;

/**
 * A Yielder is an object that tries to act like the yield() command/continuations in other languages.  It's not
 * necessarily good at this job, but it works.  I think.
 *
 * Essentially, you can think of a Yielder as a linked list of items where the Yielder gives you access to the current
 * head via get() and it will give you another Yielder representing the next item in the chain via next().  A Yielder
 * that isDone() may return anything from both get() and next(), there is no contract and depending on those return
 * values will likely lead to bugs.
 *
 * Once next is called, there is no guarantee and no requirement that references to old Yielder objects will continue
 * to obey the contract.
 *
 * Yielders are Closeable and *must* be closed in order to prevent resource leaks.  Once close() is called, the behavior
 * of the whole chain of Yielders is undefined.
 */
public interface Yielder<T> extends Closeable
{
  /**
   * Gets the object currently held by this Yielder.  Can be called multiple times as long as next() is not called.
   *
   * Once next() is called on this Yielder object, all further operations on this object are undefined.
   *
   * @return the currently yielded object, null if done
   */
  public T get();

  /**
   * Gets the next Yielder in the chain. The argument is used as the accumulator value to pass along to start the
   * accumulation until the next yield() call or iteration completes.
   *
   * Once next() is called on this Yielder object, all further operations on this object are undefined.
   *
   * @param initValue the initial value to pass along to start the accumulation until the next yield() call or
   *                  iteration completes.
   * @return the next Yielder in the chain, or undefined if done
   */
  public Yielder<T> next(T initValue);

  /**
   * Returns true if this is the last Yielder in the chain.  A Yielder that isDone() may return anything
   * from both get() and next(), there is no contract and depending on those return values will likely lead to bugs.
   * It will probably break your code to call next() on a Yielder that is done and expect something good from it.
   *
   * Once next() is called on this Yielder object, all further operations on this object are undefined.
   *
   * @return true if this is the last Yielder in the chain, false otherwise
   */
  public boolean isDone();
}
