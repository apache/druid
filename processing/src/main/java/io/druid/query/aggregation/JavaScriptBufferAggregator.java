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

package io.druid.query.aggregation;

import com.google.common.collect.Lists;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.List;

public class JavaScriptBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector[] selectorList;
  private final JavaScriptAggregator.ScriptAggregator script;

  public JavaScriptBufferAggregator(
      List<ObjectColumnSelector> selectorList,
      JavaScriptAggregator.ScriptAggregator script
  )
  {
    this.selectorList = Lists.newArrayList(selectorList).toArray(new ObjectColumnSelector[]{});
    this.script = script;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putDouble(position, script.reset());
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    buf.putDouble(position, script.aggregate(buf.getDouble(position), selectorList));
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getDouble(position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getDouble(position);
  }


  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return (long) buf.getDouble(position);
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    return buf.getDouble(position);
  }

  @Override
  public void close()
  {
    script.close();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selectorList", selectorList);
    inspector.visit("script", script);
  }
}
