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

package org.apache.druid.indexer;

import org.apache.druid.java.util.common.IAE;

import java.util.List;

/**
 * Various helper methods useful for checking the validity of arguments to spec constructors.
 */
public final class Checks
{
  public static <T> Property<T> checkOneNotNullOrEmpty(List<Property<T>> properties)
  {
    Property<T> nonNullProperty = null;
    for (Property<T> property : properties) {
      if (!property.isValueNullOrEmptyCollection()) {
        if (nonNullProperty == null) {
          nonNullProperty = property;
        } else {
          throw new IAE("At most one of %s must be present", properties);
        }
      }
    }
    if (nonNullProperty == null) {
      throw new IAE("At least one of %s must be present", properties);
    }
    return nonNullProperty;
  }

  /**
   * @return Non-null value, or first one if both are null. -1 is interpreted as null for historical reasons.
   */
  public static <T> Property<T> checkAtMostOneNotNull(Property<T> property1, Property<T> property2)
  {
    final Property<T> property;

    boolean isNull1 = property1.getValue() == null;
    boolean isNull2 = property2.getValue() == null;

    if (isNull1 && isNull2) {
      property = property1;
    } else if (isNull1) {
      property = property2;
    } else if (isNull2) {
      property = property1;
    } else {
      throw new IAE("At most one of [%s] or [%s] must be present", property1, property2);
    }

    return property;
  }

  /**
   * @return Non-null value, or first one if both are null. -1 is interpreted as null for historical reasons.
   */
  public static <T> Property<T> checkAtMostOneNotNull(String name1, T value1, String name2, T value2)
  {
    Property<T> property1 = new Property<>(name1, value1);
    Property<T> property2 = new Property<>(name2, value2);
    return checkAtMostOneNotNull(property1, property2);
  }

  private Checks()
  {
  }
}
