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

package org.apache.druid.k8s.overlord.common;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Base64CompressionTest
{

  @Test
  public void testBase64Compression() throws IOException
  {
    String lotsOfRepeatingCharacters = "mmmmmmmmmmmmmmmmmmmmkaaaaayyyyyyyyyyyy";
    String compressed = Base64Compression.compressBase64(lotsOfRepeatingCharacters);
    String uncompressed = Base64.getEncoder()
                                .encodeToString(lotsOfRepeatingCharacters.getBytes(StandardCharsets.UTF_8));
    assertTrue(compressed.length() < uncompressed.length());

    // now decompres this
    String result = Base64Compression.decompressBase64(compressed);
    assertEquals(lotsOfRepeatingCharacters, result);
  }

}
