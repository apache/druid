package com.metamx.druid.indexing.common.index;

/**
 * Objects that can be registered with a {@link ChatHandlerProvider} and provide http endpoints for indexing-related
 * objects. This interface is empty because it only exists to signal intent. The actual http endpoints are provided
 * through JAX-RS annotations on the {@link ChatHandler} objects.
 */
public interface ChatHandler
{
}
