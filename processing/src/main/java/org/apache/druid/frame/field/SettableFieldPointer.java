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
 * A simple {@link ReadableFieldPointer} that returns the position and the length that was set on its object.
 */
public class SettableFieldPointer implements ReadableFieldPointer
{
  long position = 0;
  long length = -1;

  /**
   * Sets the position and the length to be returned when interface's methods are called.
   */
  public void setPositionAndLength(long position, long length)
  {
    this.position = position;
    this.length = length;
  }

  @Override
  public long position()
  {
    return position;
  }

  @Override
  public long length()
  {
    return length;
  }
}
