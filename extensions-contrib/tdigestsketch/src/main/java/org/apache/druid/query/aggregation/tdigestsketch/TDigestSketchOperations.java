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

package org.apache.druid.query.aggregation.tdigestsketch;

import com.tdunning.math.stats.MergingDigest;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.nio.ByteBuffer;

public class TDigestSketchOperations
{
  // Class is not meant to be instantiated
  private TDigestSketchOperations()
  {
  }

  public static MergingDigest deserialize(Object serializedSketch)
  {
    if (serializedSketch instanceof String) {
      String str = (String) serializedSketch;
      return MergingDigest.fromBytes(ByteBuffer.wrap(StringUtils.decodeBase64(StringUtils.toUtf8(str))));
    } else if (serializedSketch instanceof byte[]) {
      return MergingDigest.fromBytes(ByteBuffer.wrap((byte[]) serializedSketch));
    } else if (serializedSketch instanceof MergingDigest) {
      return (MergingDigest) serializedSketch;
    }
    throw new IAE(
        "Object cannot be deserialized to a TDigest Sketch: "
        + serializedSketch.getClass()
    );
  }

  static byte[] toBytes(MergingDigest tDigest)
  {
    byte[] arr = new byte[tDigest.byteSize()];
    ByteBuffer result = ByteBuffer.wrap(arr);
    tDigest.asBytes(result);
    return result.array();
  }
}
