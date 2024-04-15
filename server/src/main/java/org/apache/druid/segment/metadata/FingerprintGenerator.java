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

package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.commons.codec.binary.Hex;
import org.apache.druid.guice.LazySingleton;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility to generate fingerprint for an object.
 */
@LazySingleton
public class FingerprintGenerator
{
  private final ObjectMapper objectMapper;

  @Inject
  public FingerprintGenerator(ObjectMapper objectMapper)
  {
    this.objectMapper = objectMapper;
  }

  /**
   * Generates fingerprint or hash string for an object using SHA-256 hash algorithm.
   */
  public String generateFingerprint(Object payload)
  {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] serializedObj = objectMapper.writeValueAsBytes(payload);

      digest.update(serializedObj);
      byte[] hashBytes = digest.digest();
      return Hex.encodeHexString(hashBytes);
    }
    catch (NoSuchAlgorithmException | IOException e) {
      throw new RuntimeException("Error generating object fingerprint.", e);
    }
  }
}
