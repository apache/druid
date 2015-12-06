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

package io.druid.segment;import io.druid.segment.data.IndexedInts;

/**
 */
public interface DimensionSelector
{
  /**
   * Gets all values for the row inside of an IntBuffer.  I.e. one possible implementation could be
   *
   * return IntBuffer.wrap(lookupExpansion(get());
   *
   * @return all values for the row as an IntBuffer
   */
  public IndexedInts getRow();

  /**
   * Value cardinality is the cardinality of the different occurring values.  If there were 4 rows:
   *
   * A,B
   * A
   * B
   * A
   *
   * Value cardinality would be 2.
   *
   * @return the value cardinality
   */
  public int getValueCardinality();

  /**
   * The Name is the String name of the actual field.  It is assumed that storage layers convert names
   * into id values which can then be used to get the string value.  For example
   *
   * A,B
   * A
   * A,B
   * B
   *
   * getRow() would return
   *
   * getRow(0) =&gt; [0 1]
   * getRow(1) =&gt; [0]
   * getRow(2) =&gt; [0 1]
   * getRow(3) =&gt; [1]
   *
   * and then lookupName would return:
   *
   * lookupName(0) =&gt; A
   * lookupName(1) =&gt; B
   *
   * @param id id to lookup the field name for
   * @return the field name for the given id
   */
  public String lookupName(int id);

  /**
   * The ID is the int id value of the field.
   *
   * @param name field name to look up the id for
   * @return the id for the given field name
   */
  public int lookupId(String name);
}
