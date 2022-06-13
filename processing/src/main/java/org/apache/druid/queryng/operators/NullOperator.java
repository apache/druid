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

package org.apache.druid.queryng.operators;

import com.google.common.base.Preconditions;
import org.apache.druid.queryng.fragment.FragmentContext;

import java.util.Collections;
import java.util.Iterator;

/**
 * World's simplest operator: does absolutely nothing
 * (other than check that the protocol is followed.) Used in
 * tests when we want an empty input, and for a fragment that
 * somehow ended up with no operators.
 */
public class NullOperator<T> implements Operator<T>
{
  public State state = State.START;

  public NullOperator(FragmentContext context)
  {
    context.register(this);
  }

  @Override
  public Iterator<T> open()
  {
    Preconditions.checkState(state == State.START);
    state = State.RUN;
    return Collections.emptyIterator();
  }

  @Override
  public void close(boolean cascade)
  {
    state = State.CLOSED;
  }
}
