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

package org.apache.druid.frame.field;

/**
 * Pointer to a field position in some memory. See {@link org.apache.druid.frame.write.RowBasedFrameWriter} for details
 * about the format.
 */
public interface ReadableFieldPointer
{
  /**
   * Starting position of the field.
   */
  long position();

  /**
   * Length of the field. Never necessary to read a field, since all fields can be read without knowing their
   * entire length. Provided because it may be useful for reading in a more optimal manner.
   */
  long length();
}
