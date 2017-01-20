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

package io.druid.sql.calcite.filtration;

import io.druid.java.util.common.ISE;
import io.druid.query.ordering.StringComparator;

public class BoundValue implements Comparable<BoundValue>
{
  private final String value;
  private final StringComparator comparator;

  public BoundValue(String value, StringComparator comparator)
  {
    this.value = value;
    this.comparator = comparator;
  }

  public String getValue()
  {
    return value;
  }

  public StringComparator getComparator()
  {
    return comparator;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BoundValue that = (BoundValue) o;

    if (value != null ? !value.equals(that.value) : that.value != null) {
      return false;
    }
    return comparator != null ? comparator.equals(that.comparator) : that.comparator == null;

  }

  @Override
  public int hashCode()
  {
    int result = value != null ? value.hashCode() : 0;
    result = 31 * result + (comparator != null ? comparator.hashCode() : 0);
    return result;
  }

  @Override
  public int compareTo(BoundValue o)
  {
    if (!comparator.equals(o.comparator)) {
      throw new ISE("WTF?! Comparator mismatch?!");
    }
    return comparator.compare(value, o.value);
  }

  @Override
  public String toString()
  {
    return value;
  }
}
