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

package org.apache.druid.query.aggregation;

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import java.nio.ByteBuffer;
import java.util.List;

public class JavaScriptBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector[] selectorList;
  private final JavaScriptAggregator.ScriptAggregator script;

  public JavaScriptBufferAggregator(
      List<BaseObjectColumnValueSelector> selectorList,
      JavaScriptAggregator.ScriptAggregator script
  )
  {
    this.selectorList = selectorList.toArray(new BaseObjectColumnValueSelector[0]);
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
