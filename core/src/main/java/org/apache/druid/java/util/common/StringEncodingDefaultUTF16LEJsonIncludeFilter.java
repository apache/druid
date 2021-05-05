package org.apache.druid.java.util.common;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * {@link JsonInclude} filter for {@link StringEncoding} that ignores UTF16LE, which is the typical default
 * for aggregators.
 *
 * This API works by "creative" use of equals. It requires warnings to be suppressed and also requires spotbugs
 * exclusions (see spotbugs-exclude.xml).
 */
@SuppressWarnings({"EqualsAndHashcode", "EqualsWhichDoesntCheckParameterClass"})
public class StringEncodingDefaultUTF16LEJsonIncludeFilter // lgtm [java/inconsistent-equals-and-hashcode]
{
  @Override
  public boolean equals(Object obj)
  {
    return obj == StringEncoding.UTF16LE;
  }
}
