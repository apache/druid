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

package org.apache.druid.indexer.partitions;

/**
 * Various helper methods useful for checking the validity of arguments to spec constructors.
 */
class Checks
{
  /**
   * @return Non-null value, or first one if both are null. -1 is interpreted as null for historical reasons.
   */
  @SuppressWarnings("VariableNotUsedInsideIf")  // false positive: checked for 'null' not used inside 'if
  static Property<Integer> checkAtMostOneNotNull(Property<Integer> property1, Property<Integer> property2)
  {
    final Property<Integer> property;

    boolean isNull1 = property1.getValue() == null;
    boolean isNull2 = property2.getValue() == null;

    if (isNull1 && isNull2) {
      property = property1;
    } else if (isNull1) {
      property = property2;
    } else if (isNull2) {
      property = property1;
    } else {
      throw new IllegalArgumentException(
          "At most one of " + property1.getName() + " or " + property2.getName() + " must be present"
      );
    }

    return property;
  }

  /**
   * @return Non-null value, or first one if both are null. -1 is interpreted as null for historical reasons.
   */
  static Property<Integer> checkAtMostOneNotNull(String name1, Integer value1, String name2, Integer value2)
  {
    Property<Integer> property1 = new Property<>(name1, value1);
    Property<Integer> property2 = new Property<>(name2, value2);
    return checkAtMostOneNotNull(property1, property2);
  }
}
