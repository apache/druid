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

package org.apache.druid.extendedset.intset;

import java.util.NoSuchElementException;

public final class EmptyIntIterator implements IntSet.IntIterator
{
  private static final EmptyIntIterator INSTANCE = new EmptyIntIterator();

  public static EmptyIntIterator instance()
  {
    return INSTANCE;
  }

  private EmptyIntIterator()
  {
  }

  @Override
  public boolean hasNext()
  {
    return false;
  }

  @Override
  public int next()
  {
    throw new NoSuchElementException();
  }

  @Override
  public void skipAllBefore(int element)
  {
    // nothing to skip
  }

  @Override
  public IntSet.IntIterator clone()
  {
    return new EmptyIntIterator();
  }
}
