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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class AuditInfo
{
  private final String author;
  private final String comment;
  private final String ip;

  @JsonCreator
  public AuditInfo(
      @JsonProperty("author") String author,
      @JsonProperty("comment") String comment,
      @JsonProperty("ip") String ip
  )
  {
    this.author = author;
    this.comment = comment;
    this.ip = ip;
  }

  @JsonProperty
  public String getAuthor()
  {
    return author;
  }

  @JsonProperty
  public String getComment()
  {
    return comment;
  }

  @JsonProperty
  public String getIp()
  {
    return ip;
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
    AuditInfo auditInfo = (AuditInfo) o;
    return Objects.equals(author, auditInfo.author)
           && Objects.equals(comment, auditInfo.comment)
           && Objects.equals(ip, auditInfo.ip);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(author, comment, ip);
  }

  @Override
  public String toString()
  {
    return "AuditInfo{" +
           "author='" + author + '\'' +
           ", comment='" + comment + '\'' +
           ", ip='" + ip + '\'' +
           '}';
  }
}
