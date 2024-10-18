package org.apache.druid.iceberg.input;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.DynamicConfigProviderUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.aws.glue.GlueCatalog;
import javax.annotation.Nullable;
import java.util.Map;

public class GlueIcebergCatalog extends IcebergCatalog {
    private static final String CATALOG_NAME = "glue";
    private Catalog catalog;

    public static final String TYPE_KEY = "glue";

    @JsonProperty
    private String warehousePath;

    @JsonProperty
    private Map<String, String> catalogProperties;

    @JsonProperty
    private final Boolean caseSensitive;
    private static final Logger log = new Logger(GlueIcebergCatalog.class);

    @JsonCreator
    public GlueIcebergCatalog(
            @JsonProperty("warehousePath") String warehousePath,
            @JsonProperty("catalogProperties") @Nullable
            Map<String, Object> catalogProperties,
            @JsonProperty("caseSensitive") Boolean caseSensitive,
            @JacksonInject @Json ObjectMapper mapper
    )
    {
        this.warehousePath = Preconditions.checkNotNull(warehousePath, "warehousePath cannot be null");
        this.catalogProperties = DynamicConfigProviderUtils.extraConfigAndSetStringMap(catalogProperties, DRUID_DYNAMIC_CONFIG_PROVIDER_KEY, mapper);
        this.caseSensitive = caseSensitive == null ? true : caseSensitive;
        this.catalog = retrieveCatalog();
    }

    @Override
    public Catalog retrieveCatalog() {
        if (catalog == null) {
            log.info("catalog is null, setting up default glue catalog.");
            catalog = setupGlueCatalog();
        }
        log.info("Glue catalog set [%s].", catalog.toString());
        return catalog;
    }

    private Catalog setupGlueCatalog() {
        catalog = new GlueCatalog();
        catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, warehousePath);
        catalog.initialize(CATALOG_NAME, catalogProperties);
        return catalog;
    }

    @Override
    public boolean isCaseSensitive()
    {
        return caseSensitive;
    }
}
