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

package io.druid.query.aggregation.datasketches;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.SetOpReturnState;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;

/**
 */
public class SynchronizedUnion implements Union
{
  private final Union delegate;

  public SynchronizedUnion(Union delegate)
  {
    this.delegate = delegate;
  }

  @Override
  public synchronized SetOpReturnState update(Sketch sketch)
  {
    return delegate.update(sketch);
  }

  @Override
  public synchronized SetOpReturnState update(Memory memory)
  {
    return delegate.update(memory);
  }

  @Override
  public synchronized CompactSketch getResult(boolean b, Memory memory)
  {
    return delegate.getResult(b, memory);
  }

  @Override
  public synchronized byte[] toByteArray()
  {
    return delegate.toByteArray();
  }

  @Override
  public synchronized void reset()
  {
    delegate.reset();
  }
}
