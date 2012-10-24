package com.metamx.druid.query.extraction;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public class RegexDimExtractionFn implements DimExtractionFn
{
  private static final byte CACHE_TYPE_ID = 0x1;

  private final String expr;
  private final Pattern pattern;

  @JsonCreator
  public RegexDimExtractionFn(
      @JsonProperty("expr") String expr
  )
  {
    this.expr = expr;
    this.pattern = Pattern.compile(expr);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] exprBytes = expr.getBytes();
    return ByteBuffer.allocate(1 + exprBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(exprBytes)
                     .array();
  }

  @Override
  public String apply(String dimValue)
  {
    Matcher matcher = pattern.matcher(dimValue);
    return matcher.find() ? matcher.group(1) : dimValue;
  }

  @JsonProperty("expr")
  public String getExpr()
  {
    return expr;
  }

  @Override
  public String toString()
  {
    return String.format("regex(%s)", expr);
  }
}