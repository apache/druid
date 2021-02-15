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

package org.apache.druid.segment.vector;

/**
 * Vector inspector that can supply a unique identifier of the vector to use with caching in addition to
 * sizing information
 */
public interface ReadableVectorInspector extends VectorSizeInspector
{
  /**
   * A marker value that will never be returned by "getId".
   */
  int NULL_ID = -1;

  /**
   * Returns an integer that uniquely identifies the current vector. This is useful for caching: it is safe to assume
   * nothing has changed in the vector so long as the id remains the same.
   */
  int getId();
}
