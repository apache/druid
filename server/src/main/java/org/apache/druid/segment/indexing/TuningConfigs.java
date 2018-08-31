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
package org.apache.druid.segment.indexing;

public class TuningConfigs
{
  private TuningConfigs()
  {
  }

  public static long getMaxBytesInMemoryOrDefault(final long maxBytesInMemory)
  {
    // In the main tuningConfig class constructor, we set the maxBytes to 0 if null to avoid setting
    // maxBytes to max jvm memory of the process that starts first. Instead we set the default based on
    // the actual task node's jvm memory.
    long newMaxBytesInMemory = maxBytesInMemory;
    if (maxBytesInMemory == 0) {
      newMaxBytesInMemory = TuningConfig.DEFAULT_MAX_BYTES_IN_MEMORY;
    } else if (maxBytesInMemory < 0) {
      newMaxBytesInMemory = Long.MAX_VALUE;
    }
    return newMaxBytesInMemory;
  }
}
