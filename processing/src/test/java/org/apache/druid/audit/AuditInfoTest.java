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

package org.apache.druid.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;

public class AuditInfoTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testAuditInfoEquality()
  {
    final AuditInfo auditInfo1 = new AuditInfo("druid", "id", "test equality", "127.0.0.1");
    final AuditInfo auditInfo2 = new AuditInfo("druid", "id", "test equality", "127.0.0.1");
    Assertions.assertEquals(auditInfo1, auditInfo2);
    Assertions.assertEquals(auditInfo1.hashCode(), auditInfo2.hashCode());
  }

  @Test
  public void testAuditInfoSerde() throws IOException
  {
    final AuditInfo auditInfo = new AuditInfo("author", null, "comment", "ip");
    AuditInfo deserialized = mapper.readValue(mapper.writeValueAsString(auditInfo), AuditInfo.class);
    Assertions.assertEquals(auditInfo, deserialized);

    final AuditInfo auditInfoWithIdentity = new AuditInfo("author", "identity", "comment", "ip");
    deserialized = mapper.readValue(mapper.writeValueAsString(auditInfoWithIdentity), AuditInfo.class);
    Assertions.assertEquals(auditInfoWithIdentity, deserialized);

    Assertions.assertNotEquals(auditInfo, auditInfoWithIdentity);
  }

  @Test
  @Timeout(60)
  public void testAuditEntrySerde() throws IOException
  {
    final AuditEntry original = new AuditEntry(
        "testKey",
        "testType",
        new AuditInfo("testAuthor", "testIdentity", "testComment", "127.0.0.1"),
        new RequestInfo("overlord", "GET", "/segments", "?abc=1"),
        AuditEntry.Payload.fromString("testPayload"),
        DateTimes.of("2013-01-01T00:00:00Z")
    );
    AuditEntry deserialized = mapper.readValue(mapper.writeValueAsString(original), AuditEntry.class);
    Assertions.assertEquals(original, deserialized);
  }

  @Test
  public void testAuditEntrySerdeIsBackwardsCompatible() throws IOException
  {
    final String json = "{\"key\": \"a\", \"type\": \"b\", \"auditInfo\": {}, \"payload\":\"Truncated\"}";
    AuditEntry entry = mapper.readValue(json, AuditEntry.class);
    Assertions.assertEquals("a", entry.getKey());
    Assertions.assertEquals("b", entry.getType());
    Assertions.assertEquals(AuditEntry.Payload.fromString("Truncated"), entry.getPayload());
  }

  @Test
  public void testRequestInfoEquality() throws IOException
  {
    RequestInfo requestInfo = new RequestInfo("overlord", "GET", "/uri", "a=b");
    RequestInfo deserialized = mapper.readValue(mapper.writeValueAsString(requestInfo), RequestInfo.class);
    Assertions.assertEquals(requestInfo, deserialized);
  }

}
