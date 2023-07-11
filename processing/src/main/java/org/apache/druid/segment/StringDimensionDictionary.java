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

package org.apache.druid.segment;

/**
 * DimensionDictionary for String dimension values.
 */
public class StringDimensionDictionary extends DimensionDictionary<String>
{
  private final boolean computeOnHeapSize;

  /**
   * Creates a StringDimensionDictionary.
   *
   * @param computeOnHeapSize true if on-heap memory estimation of the dictionary
   *                          size should be enabled, false otherwise
   */
  public StringDimensionDictionary(boolean computeOnHeapSize)
  {
    super(String.class);
    this.computeOnHeapSize = computeOnHeapSize;
  }

  @Override
  public long estimateSizeOfValue(String value)
  {
    // According to https://www.ibm.com/developerworks/java/library/j-codetoheap/index.html
    // Total string size = 28B (string metadata) + 16B (char array metadata) + 2B * num letters
    return 28 + 16 + (2L * value.length());
  }

  @Override
  public boolean computeOnHeapSize()
  {
    return computeOnHeapSize;
  }
}
