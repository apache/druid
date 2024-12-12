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

package org.apache.druid.query.lookup;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.query.extraction.MapBasedLookupExtractorTest;
import org.junit.Test;

import java.util.Map;

public class ImmutableLookupMapTest extends MapBasedLookupExtractorTest
{
  @Override
  protected LookupExtractor makeLookupExtractor(Map<String, String> map)
  {
    return ImmutableLookupMap.fromMap(map).asLookupExtractor(false, () -> new byte[0]);
  }

  @Test
  public void test_equalsAndHashCode()
  {
    EqualsVerifier.forClass(ImmutableLookupMap.class)
                  .withNonnullFields("asMap")
                  .withIgnoredFields("keyToEntry", "keys", "values")
                  .verify();
  }
}
