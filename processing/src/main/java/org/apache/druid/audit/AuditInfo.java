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

/**
 * Contains information about the author who performed an audited operation.
 */
public class AuditInfo
{
  private final String author;
  private final String identity;
  private final String comment;
  private final String ip;

  @JsonCreator
  public AuditInfo(
      @JsonProperty("author") String author,
      @JsonProperty("identity") String identity,
      @JsonProperty("comment") String comment,
      @JsonProperty("ip") String ip
  )
  {
    this.author = author;
    this.identity = identity;
    this.comment = comment;
    this.ip = ip;
  }

  @JsonProperty
  public String getAuthor()
  {
    return author;
  }

  @JsonProperty
  public String getIdentity()
  {
    return identity;
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
    AuditInfo that = (AuditInfo) o;
    return Objects.equals(this.author, that.author)
           && Objects.equals(this.identity, that.identity)
           && Objects.equals(this.comment, that.comment)
           && Objects.equals(this.ip, that.ip);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(author, identity, comment, ip);
  }

  @Override
  public String toString()
  {
    return "AuditInfo{" +
           "author='" + author + '\'' +
           ", identity='" + identity + '\'' +
           ", comment='" + comment + '\'' +
           ", ip='" + ip + '\'' +
           '}';
  }
}
