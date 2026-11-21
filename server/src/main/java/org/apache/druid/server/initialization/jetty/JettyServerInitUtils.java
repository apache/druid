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

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.servlet.GuiceFilter;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.http.client.HttpClientConfig.CompressionCodec;
import org.apache.druid.server.initialization.HttpServerCompression;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.AllowHttpMethodsResourceFilter;
import org.eclipse.jetty.compression.gzip.GzipCompression;
import org.eclipse.jetty.compression.gzip.GzipDecoderConfig;
import org.eclipse.jetty.compression.gzip.GzipEncoderConfig;
import org.eclipse.jetty.compression.server.CompressionConfig;
import org.eclipse.jetty.compression.server.CompressionHandler;
import org.eclipse.jetty.compression.zstandard.ZstandardCompression;
import org.eclipse.jetty.compression.zstandard.ZstandardEncoderConfig;
import org.eclipse.jetty.ee8.servlet.FilterHolder;
import org.eclipse.jetty.ee8.servlet.FilterMapping;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.rewrite.handler.HeaderPatternRule;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Handler;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class JettyServerInitUtils
{
  /**
   * Wraps the handler with a {@link CompressionHandler} that compresses responses to GET and POST requests and
   * decompresses request bodies.
   *
   * The response encoding is picked from the request's {@code Accept-Encoding} header: among the encodings the client
   * accepts, the first one in {@link HttpServerCompression#getCodecs()} is used. If the client accepts none of them,
   * the response is not compressed. {@link NormalizeAcceptEncodingHandler} works around two Jetty negotiation bugs, so
   * that encoding names match ignoring case and a {@code *} entry never picks an encoding the client declined.
   *
   * Request bodies may be gzip-compressed, or zstd-compressed if zstd is enabled in
   * {@link HttpServerCompression#getCodecs()}. zstd is not registered at all when it is disabled, so that disabling it
   * also avoids loading its native library.
   *
   * {@link HttpServerCompression.Gzip#inflateBufferSizeOrDefault()} sets the buffer size of the gzip request
   * decoder. If it is 0, request bodies are not decompressed, for any codec, as with the old {@code GzipHandler}.
   */
  public static Handler wrapWithCompressionHandler(
      final ServletContextHandler handler,
      final HttpServerCompression compressionConfig
  )
  {
    final CompressionHandler compressionHandler = createCompressionHandler(compressionConfig);

    // defaults() compresses responses to GET and POST and skips already-compressed types. It decompresses only POST
    // bodies, and its method include list cannot be cleared, so include every method Jetty knows, as the old
    // GzipHandler inflated request bodies of any method.
    final CompressionConfig.Builder config = CompressionConfig.builder()
                                                              .defaults()
                                                              .compressPreferredEncodings(compressionConfig.getCodecNames());
    for (HttpMethod method : HttpMethod.values()) {
      config.decompressIncludeMethod(method.asString());
    }
    if (!compressionConfig.getCodecs().contains(CompressionCodec.GZIP)) {
      // gzip stays registered for request decompression, so exclude it from response compression.
      config.compressExcludeEncoding(CompressionCodec.GZIP.getEncodingString());
    }
    if (compressionConfig.getGzip().inflateBufferSizeOrDefault() == 0) {
      // The old GzipHandler did not inflate request bodies when inflateBufferSize was 0.
      config.decompressExcludePath("/*");
    }
    compressionHandler.putConfiguration("/", config.build());

    return NormalizeAcceptEncodingHandler.wrap(compressionHandler, handler.get(), compressionConfig.getCodecNames());
  }

  private static CompressionHandler createCompressionHandler(final HttpServerCompression compressionConfig)
  {
    final CompressionHandler compressionHandler = new CompressionHandler();

    // gzip is always registered, even if it is not in the codecs list, so that gzip-compressed request bodies are
    // still decompressed, as with the old GzipHandler. The codecs list only controls response compression, which
    // wrapWithCompressionHandler turns off for gzip if it is not listed.
    final GzipCompression gzip = new GzipCompression();
    final GzipEncoderConfig gzipEncoderConfig = new GzipEncoderConfig();
    gzipEncoderConfig.setCompressionLevel(compressionConfig.getGzip().levelOrDefault());
    gzip.setDefaultEncoderConfig(gzipEncoderConfig);

    final int inflateBufferSize = compressionConfig.getGzip().inflateBufferSizeOrDefault();
    if (inflateBufferSize > 0) {
      final GzipDecoderConfig gzipDecoderConfig = new GzipDecoderConfig();
      gzipDecoderConfig.setBufferSize(inflateBufferSize);
      gzip.setDefaultDecoderConfig(gzipDecoderConfig);
    }
    gzip.setMinCompressSize(0);
    compressionHandler.putCompression(gzip);

    if (compressionConfig.getCodecs().contains(CompressionCodec.ZSTD)) {
      final ZstandardCompression zstd = new ZstandardCompression();
      final ZstandardEncoderConfig zstdEncoderConfig = new ZstandardEncoderConfig();
      zstdEncoderConfig.setCompressionLevel(compressionConfig.getZstd().level());
      zstd.setDefaultEncoderConfig(zstdEncoderConfig);
      zstd.setMinCompressSize(0);
      compressionHandler.putCompression(zstd);
    }

    return compressionHandler;
  }

  /**
   * Add any filters that were registered with {@link JettyBindings#addQosFilter}. These must be added first in
   * the filter chain, because when a request is suspended and later resumed due to QoS constraints, its filter
   * chain is restarted. Placing QoSFilters first in the chain avoids double-execution of other filters.
   */
  public static void addQosFilters(ServletContextHandler handler, Injector injector)
  {
    final Set<JettyBindings.QosFilterHolder> filters =
        injector.getInstance(Key.get(new TypeLiteral<>() {}));
    addFilters(handler, filters);
  }

  public static void addExtensionFilters(ServletContextHandler handler, Injector injector)
  {
    final Set<ServletFilterHolder> filters =
        injector.getInstance(Key.get(new TypeLiteral<>() {}));
    addFilters(handler, filters);
  }

  public static void addFilters(ServletContextHandler handler, Set<? extends ServletFilterHolder> filterHolders)
  {
    for (ServletFilterHolder servletFilterHolder : filterHolders) {
      // Check the Filter first to guard against people who don't read the docs and return the Class even
      // when they have an instance.
      FilterHolder holder;
      if (servletFilterHolder.getFilter() != null) {
        holder = new FilterHolder(servletFilterHolder.getFilter());
      } else if (servletFilterHolder.getFilterClass() != null) {
        holder = new FilterHolder(servletFilterHolder.getFilterClass());
      } else {
        throw new ISE(
            "Filter[%s] for paths[%s] didn't have a Filter!?",
            servletFilterHolder,
            Arrays.toString(servletFilterHolder.getPaths())
        );
      }

      if (servletFilterHolder.getInitParameters() != null) {
        holder.setInitParameters(servletFilterHolder.getInitParameters());
      }

      FilterMapping filterMapping = new FilterMapping();
      filterMapping.setFilterName(holder.getName());
      filterMapping.setPathSpecs(servletFilterHolder.getPaths());
      filterMapping.setDispatcherTypes(servletFilterHolder.getDispatcherType());

      handler.getServletHandler().addFilter(holder, filterMapping);
    }
  }

  public static void addAllowHttpMethodsFilter(ServletContextHandler root, List<String> allowedHttpMethods)
  {
    FilterHolder holder = new FilterHolder(new AllowHttpMethodsResourceFilter(allowedHttpMethods));
    root.addFilter(
        holder,
        "/*",
        null
    );
  }
  
  public static void maybeAddHSTSPatternRule(ServerConfig serverConfig, RewriteHandler rewriteHandler)
  {
    if (serverConfig.isEnableHSTS()) {
      rewriteHandler.addRule(getHSTSHeaderPattern());
    }
  }

  public static void maybeAddHSTSRewriteHandler(ServerConfig serverConfig, Handler.Sequence handlerList)
  {
    if (serverConfig.isEnableHSTS()) {
      RewriteHandler rewriteHandler = new RewriteHandler();
      rewriteHandler.addRule(getHSTSHeaderPattern());
      handlerList.addHandler(rewriteHandler);
    }
  }

  /**
   * Using this {@link FilterHolder} forces the underlying {@link GuiceFilter}
   * to be instantiated with the 1-argument constructor that uses an injected
   * non-static FilterPipeline. Since the FilterPipeline is non-static, multiple
   * Jetty servers may be initialized by separate Guice Injectors running in the
   * same JVM. This is currently needed only for running embedded tests.
   *
   * @see GuiceFilter#GuiceFilter
   */
  public static FilterHolder getGuiceFilterHolder(Injector injector)
  {
    return new FilterHolder(injector.getInstance(GuiceFilter.class));
  }

  private static HeaderPatternRule getHSTSHeaderPattern()
  {
    return new HeaderPatternRule("*", "Strict-Transport-Security", "max-age=63072000; includeSubDomains");
  }
}
