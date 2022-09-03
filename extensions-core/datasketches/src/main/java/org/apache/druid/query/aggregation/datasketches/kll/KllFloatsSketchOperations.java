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

package org.apache.druid.query.aggregation.datasketches.kll;

import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;

import java.nio.charset.StandardCharsets;

public class KllFloatsSketchOperations
{

  public static final KllFloatsSketch EMPTY_SKETCH = KllFloatsSketch.newHeapInstance();

  public static KllFloatsSketch deserialize(final Object serializedSketch)
  {
    if (serializedSketch instanceof String) {
      return deserializeFromBase64EncodedString((String) serializedSketch);
    } else if (serializedSketch instanceof byte[]) {
      return deserializeFromByteArray((byte[]) serializedSketch);
    } else if (serializedSketch instanceof KllFloatsSketch) {
      return (KllFloatsSketch) serializedSketch;
    }
    throw new ISE(
        "Object is not of a type that can be deserialized to a KllFloatsSketch: %s",
        serializedSketch == null ? "null" : serializedSketch.getClass()
    );
  }

  public static KllFloatsSketch deserializeFromBase64EncodedString(final String str)
  {
    return deserializeFromByteArray(StringUtils.decodeBase64(str.getBytes(StandardCharsets.UTF_8)));
  }

  public static KllFloatsSketch deserializeFromByteArray(final byte[] data)
  {
    return KllFloatsSketch.wrap(Memory.wrap(data));
  }

}
