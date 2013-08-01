package com.metamx.druid.http.log;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.inject.Provider;

/**
 * A Marker interface for things that can provide a RequestLogger.  This can be combined with jackson polymorphic serde
 * to provide new RequestLogger implementations as plugins.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = NoopRequestLoggerProvider.class)
public interface RequestLoggerProvider extends Provider<RequestLogger>
{
}
