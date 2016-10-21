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

import com.google.common.primitives.Ints;
import io.druid.java.util.common.IAE;

import java.io.IOException;
import java.util.List;

public abstract class MultiValueIndexedIntsWriter implements IndexedIntsWriter
{
  @Override
  public void add(Object obj) throws IOException
  {
    if (obj == null) {
      addValues(null);
    } else if (obj instanceof int[]) {
      addValues(Ints.asList((int[]) obj));
    } else if (obj instanceof List) {
      addValues((List<Integer>) obj);
    } else {
      throw new IAE("unsupported multi-value type: " + obj.getClass());
    }
  }

  protected abstract void addValues(List<Integer> vals) throws IOException;
}
