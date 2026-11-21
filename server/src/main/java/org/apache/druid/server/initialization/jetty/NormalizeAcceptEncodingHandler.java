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

package org.apache.druid.server.initialization.jetty;

import org.apache.druid.java.util.common.StringUtils;
import org.eclipse.jetty.compression.server.CompressionHandler;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.QuotedQualityCSV;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Shows Jetty's {@link CompressionHandler} a normalized {@code Accept-Encoding} header, to work around two bugs in its
 * response encoding negotiation ({@code CompressionConfig.getCompressionEncoding}, as of Jetty 12.1.15):
 *
 * <table>
 *   <caption>Negotiation in Jetty</caption>
 *   <tr><th>Request {@code Accept-Encoding}</th><th>Server codecs</th><th>Expected</th><th>Jetty picks</th></tr>
 *   <tr><td>{@code zstd;q=0, *;q=1}</td><td>{@code ["zstd", "gzip"]}</td><td>gzip</td><td>zstd</td></tr>
 *   <tr><td>{@code br, GZIP}</td><td>{@code []}</td><td>not compressed</td><td>gzip</td></tr>
 * </table>
 *
 * <ul>
 *   <li>Jetty skips entries with {@code q=0}, but does not remember them. If no other entry matches, it uses the most
 *   preferred encoding for {@code *}, even one the client just declined. RFC 9110 section 12.5.3 says {@code q=0} means
 *   "not acceptable" and {@code *} only covers encodings the header does not list.</li>
 *   <li>Jetty looks up encodings ignoring case, but compares them with excluded encodings case-sensitively, so
 *   {@code GZIP} is not excluded by {@code compressExcludeEncoding("gzip")}.</li>
 * </ul>
 *
 * {@link #normalizeAcceptEncoding} rewrites the header to avoid both. {@link RestoreHandler}, placed between the
 * {@link CompressionHandler} and the handler it wraps, then restores the header the client sent, so that servlets,
 * including the proxy servlets that forward the header, see it unchanged. Use {@link #wrap} to set up both.
 *
 * This class can be removed once Jetty fixes these bugs.
 */
public class NormalizeAcceptEncodingHandler extends Handler.Wrapper
{
  private static final String ORIGINAL_ACCEPT_ENCODING_ATTRIBUTE =
      NormalizeAcceptEncodingHandler.class.getName() + ".originalAcceptEncoding";

  private final List<String> codecNames;

  private NormalizeAcceptEncodingHandler(final CompressionHandler compressionHandler, final List<String> codecNames)
  {
    super(compressionHandler);
    this.codecNames = codecNames;
  }

  /**
   * Sets {@code handler} as the handler of {@code compressionHandler}, and returns a handler that shows
   * {@code compressionHandler} a normalized {@code Accept-Encoding} header and {@code handler} the original one.
   *
   * @param codecNames the server's response encodings, most preferred first
   */
  public static Handler wrap(
      final CompressionHandler compressionHandler,
      final Handler handler,
      final List<String> codecNames
  )
  {
    compressionHandler.setHandler(new RestoreHandler(handler));
    return new NormalizeAcceptEncodingHandler(compressionHandler, codecNames);
  }

  @Override
  public boolean handle(final Request request, final Response response, final Callback callback) throws Exception
  {
    final List<HttpField> original = request.getHeaders().getFields(HttpHeader.ACCEPT_ENCODING);
    if (original.isEmpty()) {
      return super.handle(request, response, callback);
    }

    final String normalized = normalizeAcceptEncoding(original.stream().map(HttpField::getValue).toList(), codecNames);
    final HttpFields.Mutable headers = HttpFields.build(request.getHeaders(), EnumSet.of(HttpHeader.ACCEPT_ENCODING));
    if (normalized != null) {
      headers.add(HttpHeader.ACCEPT_ENCODING, normalized);
    }
    request.setAttribute(ORIGINAL_ACCEPT_ENCODING_ATTRIBUTE, original);
    return super.handle(new HeadersRequest(request, headers.asImmutable()), response, callback);
  }

  /**
   * Rewrites {@code Accept-Encoding} header values so that Jetty's negotiation honors them:
   * <ul>
   *   <li>Encoding names are lowercased.</li>
   *   <li>A {@code *} entry is replaced by an entry, with the same quality, for each of {@code codecNames} that the
   *   header does not name.</li>
   * </ul>
   *
   * @return the normalized value, or null if no entry is left
   */
  @Nullable
  static String normalizeAcceptEncoding(final List<String> headerValues, final List<String> codecNames)
  {
    final QuotedQualityCSV parsed = new QuotedQualityCSV();
    headerValues.forEach(parsed::addValue);

    final List<String> entries = new ArrayList<>();
    final Set<String> named = new HashSet<>();
    QuotedQualityCSV.QualityValue star = null;
    for (QuotedQualityCSV.QualityValue value : parsed.getQualityValues()) {
      if ("*".equals(value.getValue())) {
        star = value;
      } else {
        final String name = StringUtils.toLowerCase(value.getValue());
        named.add(name);
        entries.add(formatEntry(name, value.getWeight()));
      }
    }
    if (star != null) {
      for (String codec : codecNames) {
        if (!named.contains(codec)) {
          entries.add(formatEntry(codec, star.getWeight()));
        }
      }
    }
    return entries.isEmpty() ? null : String.join(", ", entries);
  }

  private static String formatEntry(final String name, final double weight)
  {
    return weight == 1.0 ? name : name + ";q=" + weight;
  }

  /**
   * Restores the {@code Accept-Encoding} header that {@link NormalizeAcceptEncodingHandler} replaced.
   */
  private static class RestoreHandler extends Handler.Wrapper
  {
    RestoreHandler(final Handler handler)
    {
      super(handler);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean handle(final Request request, final Response response, final Callback callback) throws Exception
    {
      final List<HttpField> original = (List<HttpField>) request.removeAttribute(ORIGINAL_ACCEPT_ENCODING_ATTRIBUTE);
      if (original == null) {
        return super.handle(request, response, callback);
      }

      final HttpFields.Mutable headers = HttpFields.build(request.getHeaders(), EnumSet.of(HttpHeader.ACCEPT_ENCODING));
      original.forEach(headers::add);
      return super.handle(new HeadersRequest(request, headers.asImmutable()), response, callback);
    }
  }

  private static class HeadersRequest extends Request.Wrapper
  {
    private final HttpFields headers;

    HeadersRequest(final Request request, final HttpFields headers)
    {
      super(request);
      this.headers = headers;
    }

    @Override
    public HttpFields getHeaders()
    {
      return headers;
    }
  }
}
