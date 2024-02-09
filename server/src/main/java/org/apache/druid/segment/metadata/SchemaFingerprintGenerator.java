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

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SchemaFingerprintGenerator
{

  private final ObjectMapper objectMapper;

  @Inject
  public SchemaFingerprintGenerator(ObjectMapper objectMapper)
  {
    this.objectMapper = objectMapper;
  }

  public String generateId(Object payload)
  {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] serializedObj = objectMapper.writeValueAsBytes(payload);

      digest.update(serializedObj);
      byte[] hashBytes = digest.digest();
      return bytesToHex(hashBytes);
    }
    catch (NoSuchAlgorithmException | IOException e) {
      throw new RuntimeException("Error generating object hash", e);
    }
  }

  private String bytesToHex(byte[] bytes)
  {
    StringBuilder hexString = new StringBuilder();
    for (byte b : bytes) {
      String hex = Integer.toHexString(0xff & b);
      if (hex.length() == 1) {
        hexString.append('0');
      }
      hexString.append(hex);
    }
    return hexString.toString();
  }
}
