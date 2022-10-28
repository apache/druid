package org.apache.druid.catalog.sync;

/**
 * Generic interface for changes to the catalog at the storage level.
 * Implemented by the catalog sync mechanism to send update events
 * to the Broker. Note that these events are about the <i>catalog</li>,
 * not about the physical storage of tables (i.e. datasources.)
 */
public interface CatalogUpdateListener
{
  void updated(UpdateEvent event);
}