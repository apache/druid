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

/**
 * A YieldingAccumulator is used along with a Yielder in order to replicate continuations in Java.  I'm still not sure
 * this is such a great idea, but it's there.  We shall see.
 *
 * The accumulated has its accumulate() method called and has the option of "yielding" its response by calling
 * yield() before returning as response.  If it chooses not to yield its response, then it expects to get called
 * again with the next value and the value it just returned.
 */
public abstract class YieldingAccumulator<AccumulatedType, InType>
{
  private boolean yielded = false;

  public void yield()
  {
    yielded = true;
  }

  public boolean yielded()
  {
    return yielded;
  }

  public void reset()
  {
    yielded = false;
  }

  public abstract AccumulatedType accumulate(AccumulatedType accumulated, InType in);
}
