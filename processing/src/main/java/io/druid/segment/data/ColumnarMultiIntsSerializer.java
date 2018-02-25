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

package io.druid.segment.data;

import io.druid.java.util.common.IAE;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;

/**
 * Serializer that produces {@link ColumnarMultiInts}.
 */
public abstract class ColumnarMultiIntsSerializer implements ColumnarIntsSerializer
{
  @Override
  public void add(Object obj) throws IOException
  {
    if (obj == null) {
      addValues(null);
    } else if (obj instanceof int[]) {
      addValues(IntArrayList.wrap((int[]) obj));
    } else if (obj instanceof IntList) {
      addValues((IntList) obj);
    } else {
      throw new IAE("unsupported multi-value type: " + obj.getClass());
    }
  }

  protected abstract void addValues(IntList vals) throws IOException;
}
