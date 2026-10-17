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

package org.apache.druid.query.context.docs;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ParameterDocumentationTest
{
  @Test
  void testDescriptionsAreNormalizedToOneLine()
  {
    final ParameterDocumentation docs = ParameterDocumentation
        .builder()
        .description(" Description with \\\n wrapped lines \n and   a continuation marker. ")
        .defaultDescription("""
                            The value of
                            `druid.some.property`
                            """)
        .build();

    assertEquals("Description with wrapped lines and a continuation marker.", docs.getDescription());
    assertEquals("The value of `druid.some.property`", docs.getDefaultDescription().orElseThrow());
  }
}
