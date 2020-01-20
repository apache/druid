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

package org.apache.druid.query.aggregation.datasketches.quantiles;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;

import java.nio.charset.StandardCharsets;

public class DoublesSketchOperations
{

  public static final DoublesSketch EMPTY_SKETCH = DoublesSketch.builder().build();

  public static DoublesSketch deserialize(final Object serializedSketch)
  {
    if (serializedSketch instanceof String) {
      return deserializeFromBase64EncodedString((String) serializedSketch);
    } else if (serializedSketch instanceof byte[]) {
      return deserializeFromByteArray((byte[]) serializedSketch);
    } else if (serializedSketch instanceof DoublesSketch) {
      return (DoublesSketch) serializedSketch;
    }
    throw new ISE(
        "Object is not of a type that can be deserialized to a quantiles DoublsSketch: "
            + serializedSketch.getClass());
  }

  public static DoublesSketch deserializeFromBase64EncodedString(final String str)
  {
    return deserializeFromByteArray(StringUtils.decodeBase64(str.getBytes(StandardCharsets.UTF_8)));
  }

  public static DoublesSketch deserializeFromByteArray(final byte[] data)
  {
    return DoublesSketch.wrap(Memory.wrap(data));
  }

}
