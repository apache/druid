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

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.List;


public class ComparableList<T extends Comparable> implements Comparable<ComparableList>
{

  private final List<T> delegate;

  public ComparableList(List<T> input)
  {
    delegate = input;
  }

  @JsonValue
  public List<T> getDelegate()
  {
    return delegate;
  }

  @Override
  public int hashCode()
  {
    return delegate.hashCode();
  }


  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    return this.delegate.equals(((ComparableList) obj).getDelegate());
  }

  @Override
  public int compareTo(ComparableList o)
  {
    if (delegate.size() > o.getDelegate().size()) {
      return 1;
    } else if (delegate.size() < o.getDelegate().size()) {
      return -1;
    } else {
      for (int i = 0; i < delegate.size(); i++) {
        final int cmp = delegate.get(i).compareTo(o.getDelegate().get(i));
        if (cmp == 0) {
          continue;
        } else {
          return cmp;
        }
      }
      return 0;
    }
  }
}
