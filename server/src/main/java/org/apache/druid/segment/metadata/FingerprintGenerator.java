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
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.SchemaPayload;

import java.io.IOException;

/**
 * Utility to generate fingerprint for an object.
 */
@LazySingleton
public class FingerprintGenerator
{
  private static final Logger log = new Logger(FingerprintGenerator.class);

  private final ObjectMapper objectMapper;

  @Inject
  public FingerprintGenerator(ObjectMapper objectMapper)
  {
    this.objectMapper = objectMapper;
  }

  /**
   * Generates fingerprint or hash string for an object using SHA-256 hash algorithm.
   */
  @SuppressWarnings("UnstableApiUsage")
  public String generateFingerprint(SchemaPayload schemaPayload, String dataSource, int version)
  {
    try {
      final Hasher hasher = Hashing.sha256().newHasher();

      hasher.putBytes(objectMapper.writeValueAsBytes(schemaPayload));
      // add delimiter, inspired from org.apache.druid.metadata.PendingSegmentRecord.computeSequenceNamePrevIdSha1
      hasher.putByte((byte) 0xff);

      hasher.putBytes(StringUtils.toUtf8(dataSource));
      hasher.putByte((byte) 0xff);

      hasher.putBytes(Ints.toByteArray(version));
      hasher.putByte((byte) 0xff);

      return BaseEncoding.base16().encode(hasher.hash().asBytes());
    }
    catch (IOException e) {
      log.error(
          e,
          "Exception generating schema fingerprint (version[%d]) for datasource[%s], payload[%s].",
          version, dataSource, schemaPayload
      );
      throw DruidException.defensive(
          "Could not generate schema fingerprint (version[%d]) for datasource[%s].",
          dataSource, version
      );
    }
  }
}
