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

package org.apache.druid.query.rowsandcols.column;

/**
 * A semantic interface for use with {@link Column} objects.  Has methods to request that the {@link Column} copy
 * values into provided array structures.  Note that this interface is primarily useful to <em>read</em> from
 * {@link Column} objects and not to write to them.
 */
public interface VectorCopier
{
  /**
   * Copies all values from the underlying column <em>into</em> the {@code Object[]} passed into this method.
   *
   * @param into the object array to store the values
   * @param intoStart the index of the into array to start writing into.
   */
  void copyInto(Object[] into, int intoStart);
}
