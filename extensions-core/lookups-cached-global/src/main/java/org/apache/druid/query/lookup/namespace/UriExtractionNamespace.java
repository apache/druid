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

package org.apache.druid.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.lookup.namespace.parsers.FlatDataParser;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import java.net.URI;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 *
 */
@JsonTypeName("uri")
public class UriExtractionNamespace implements ExtractionNamespace
{
  @JsonProperty
  private final URI uri;
  @JsonProperty
  private final URI uriPrefix;
  @JsonProperty
  private final FlatDataParser namespaceParseSpec;
  @JsonProperty
  private final String fileRegex;
  @JsonProperty
  private final Period pollPeriod;

  @JsonCreator
  public UriExtractionNamespace(
      @JsonProperty(value = "uri", required = false)
          URI uri,
      @JsonProperty(value = "uriPrefix", required = false)
          URI uriPrefix,
      @JsonProperty(value = "fileRegex", required = false)
          String fileRegex,
      @JsonProperty(value = "namespaceParseSpec", required = true)
          FlatDataParser namespaceParseSpec,
      @Min(0) @Nullable @JsonProperty(value = "pollPeriod", required = false)
          Period pollPeriod,
      @Deprecated
      @JsonProperty(value = "versionRegex", required = false)
          String versionRegex
  )
  {
    this.uri = uri;
    this.uriPrefix = uriPrefix;
    if ((uri != null) == (uriPrefix != null)) {
      throw new IAE("Either uri xor uriPrefix required");
    }
    this.namespaceParseSpec = Preconditions.checkNotNull(namespaceParseSpec, "namespaceParseSpec");
    this.pollPeriod = pollPeriod == null ? Period.ZERO : pollPeriod;
    this.fileRegex = fileRegex == null ? versionRegex : fileRegex;
    if (fileRegex != null && versionRegex != null) {
      throw new IAE("Cannot specify both versionRegex and fileRegex. versionRegex is deprecated");
    }

    if (uri != null && this.fileRegex != null) {
      throw new IAE("Cannot define both uri and fileRegex");
    }

    if (this.fileRegex != null) {
      try {
        Pattern.compile(this.fileRegex);
      }
      catch (PatternSyntaxException ex) {
        throw new IAE(ex, "Could not parse `fileRegex` [%s]", this.fileRegex);
      }
    }
  }

  public String getFileRegex()
  {
    return fileRegex;
  }

  public FlatDataParser getNamespaceParseSpec()
  {
    return this.namespaceParseSpec;
  }

  public URI getUri()
  {
    return uri;
  }

  public URI getUriPrefix()
  {
    return uriPrefix;
  }

  @Override
  public long getPollMs()
  {
    return pollPeriod.toStandardDuration().getMillis();
  }

  @Override
  public String toString()
  {
    return "UriExtractionNamespace{" +
           "uri=" + uri +
           ", uriPrefix=" + uriPrefix +
           ", namespaceParseSpec=" + namespaceParseSpec +
           ", fileRegex='" + fileRegex + '\'' +
           ", pollPeriod=" + pollPeriod +
           '}';
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

    UriExtractionNamespace that = (UriExtractionNamespace) o;

    if (getUri() != null ? !getUri().equals(that.getUri()) : that.getUri() != null) {
      return false;
    }
    if (getUriPrefix() != null ? !getUriPrefix().equals(that.getUriPrefix()) : that.getUriPrefix() != null) {
      return false;
    }
    if (!getNamespaceParseSpec().equals(that.getNamespaceParseSpec())) {
      return false;
    }
    if (getFileRegex() != null ? !getFileRegex().equals(that.getFileRegex()) : that.getFileRegex() != null) {
      return false;
    }
    return pollPeriod.equals(that.pollPeriod);

  }

  @Override
  public int hashCode()
  {
    int result = getUri() != null ? getUri().hashCode() : 0;
    result = 31 * result + (getUriPrefix() != null ? getUriPrefix().hashCode() : 0);
    result = 31 * result + getNamespaceParseSpec().hashCode();
    result = 31 * result + (getFileRegex() != null ? getFileRegex().hashCode() : 0);
    result = 31 * result + pollPeriod.hashCode();
    return result;
  }

}
