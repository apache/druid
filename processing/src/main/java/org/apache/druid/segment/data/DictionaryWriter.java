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

package org.apache.druid.segment.data;

import org.apache.druid.segment.serde.Serializer;

import javax.annotation.Nullable;
import java.io.IOException;

public interface DictionaryWriter<T> extends Serializer
{
  boolean isSorted();

  /**
   * Prepares the writer for writing
   *
   * @throws IOException if there is a problem with IO
   */
  void open() throws IOException;

  /**
   * Writes an object to the dictionary.
   * <p>
   * Returns the index of the value that was just written.  This is defined as the `int` value that can be passed
   * into {@link #get} such that it will return the same value back.
   *
   * @param objectToWrite object to be written to the dictionary
   * @return index of the value that was just written
   * @throws IOException if there is a problem with IO
   */
  int write(@Nullable T objectToWrite) throws IOException;

  /**
   * Returns an object that has already been written via the {@link #write} method.
   *
   * @param dictId index of the object to return
   * @return the object identified by the given index
   * @throws IOException if there is a problem with IO
   */
  @Nullable
  T get(int dictId) throws IOException;

  /**
   * Returns the number of items that have been written so far in this dictionary.  Any number lower than this
   * cardinality can be passed into {@link #get} and a value will be returned.  If a value greater than or equal to
   * the cardinality is passed into {@link #get} all sorts of things could happen, but likely none of them are good.
   *
   * @return the number of items that have been written so far
   */
  int getCardinality();
}
