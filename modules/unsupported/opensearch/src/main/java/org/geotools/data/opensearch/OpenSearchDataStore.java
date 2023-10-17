/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2020, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package org.geotools.data.opensearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.http.HttpHost;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.opensearch.OpenSearchAttribute.OpenSearchGeometryType;
import org.geotools.data.opensearch.date.ElasticsearchDateConverter;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.type.Name;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;

/** A data store for an OpenSearch index containing geo_point or geo_shape types. */
public class OpenSearchDataStore extends ContentDataStore {

    private static final Logger LOGGER = Logging.getLogger(OpenSearchDataStore.class);

    private OpenSearchClient client;

    private final String indexName;

    private final List<Name> baseTypeNames;

    private final Map<Name, String> docTypes;

    private Map<String, OpenSearchLayerConfiguration> layerConfigurations;

    private boolean sourceFilteringEnabled;

    private Integer defaultMaxFeatures;

    private Long scrollSize;

    private boolean scrollEnabled;

    private Integer scrollTime;

    private ArrayEncoding arrayEncoding;

    private Long gridSize;

    private Double gridThreshold;

    public enum ArrayEncoding {

        /** Return all arrays without encoding. */
        JSON,

        /** URL encode and join string array elements. */
        CSV
    }

    public OpenSearchDataStore(String searchHost, Integer hostPort, String indexName)
            throws IOException {
        this(RestClient.builder(new HttpHost(searchHost, hostPort, "http")).build(), indexName);
    }

    public OpenSearchDataStore(RestClient restClient, String indexName) throws IOException {
        this(restClient, null, indexName, false);
    }

    public OpenSearchDataStore(
            RestClient restClient,
            RestClient proxyRestClient,
            String indexName,
            boolean enableRunAs)
            throws IOException {
        LOGGER.fine("Initializing data store for " + indexName);

        this.indexName = indexName;

        try {
            checkRestClient(restClient);
            if (proxyRestClient != null) {
                checkRestClient(proxyRestClient);
            }
            client = new RestOpenSearchClient(restClient, proxyRestClient, enableRunAs);
        } catch (Exception e) {
            throw new IOException("Unable to create REST client", e);
        }
        LOGGER.fine("Created REST client: " + client);

        final List<String> types = getClient().getTypes(indexName);
        if (!types.isEmpty()) {
            baseTypeNames = types.stream().map(NameImpl::new).collect(Collectors.toList());
        } else {
            baseTypeNames = new ArrayList<>();
        }

        layerConfigurations = new ConcurrentHashMap<>();
        docTypes = new HashMap<>();

        arrayEncoding = ArrayEncoding.JSON;
    }

    @Override
    protected List<Name> createTypeNames() {
        final List<Name> names = new ArrayList<>();
        names.addAll(baseTypeNames);
        names.addAll(docTypes.keySet());
        return names;
    }

    @Override
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        return new OpenSearchFeatureSource(entry, Query.ALL);
    }

    @Override
    public ContentFeatureSource getFeatureSource(Name name, Transaction tx) throws IOException {

        final OpenSearchLayerConfiguration layerConfig =
                layerConfigurations.get(name.getLocalPart());
        if (layerConfig != null) {
            docTypes.put(name, layerConfig.getDocType());
        }
        final ContentFeatureSource featureSource = super.getFeatureSource(name, tx);
        featureSource.getEntry().getState(Transaction.AUTO_COMMIT).flush();

        return featureSource;
    }

    @Override
    public void dispose() {
        try {
            this.client.close();
        } catch (IOException e) {
            throw new RuntimeException(e.getCause());
        }
        super.dispose();
    }

    public List<OpenSearchAttribute> getOpenSearchAttributes(Name layerName) throws IOException {
        final String localPart = layerName.getLocalPart();
        final OpenSearchLayerConfiguration layerConfig = layerConfigurations.get(localPart);
        final List<OpenSearchAttribute> openSearchAttributes;
        if (layerConfig == null || layerConfig.getAttributes().isEmpty()) {
            final String docType = docTypes.getOrDefault(layerName, localPart);
            final Map<String, Object> mapping = getClient().getMapping(indexName, docType);
            openSearchAttributes = new ArrayList<>();
            if (mapping != null) {
                add(openSearchAttributes, "_id", "string", mapping, false);
                add(openSearchAttributes, "_index", "string", mapping, false);
                add(openSearchAttributes, "_type", "string", mapping, false);
                add(openSearchAttributes, "_score", "float", mapping, false);
                add(openSearchAttributes, "_relative_score", "float", mapping, false);
                add(openSearchAttributes, "_aggregation", "binary", mapping, false);

                walk(openSearchAttributes, mapping, "", false, false);
            }
        } else {
            openSearchAttributes = layerConfig.getAttributes();
        }
        return openSearchAttributes;
    }

    String getIndexName() {
        return indexName;
    }

    OpenSearchClient getClient() {
        return client;
    }

    boolean isSourceFilteringEnabled() {
        return sourceFilteringEnabled;
    }

    public void setSourceFilteringEnabled(boolean sourceFilteringEnabled) {
        this.sourceFilteringEnabled = sourceFilteringEnabled;
    }

    public Integer getDefaultMaxFeatures() {
        return defaultMaxFeatures;
    }

    public void setDefaultMaxFeatures(Integer defaultMaxFeatures) {
        this.defaultMaxFeatures = defaultMaxFeatures;
    }

    public Long getScrollSize() {
        return scrollSize;
    }

    public Boolean getScrollEnabled() {
        return scrollEnabled;
    }

    public Integer getScrollTime() {
        return scrollTime;
    }

    public void setScrollSize(Long scrollSize) {
        this.scrollSize = scrollSize;
    }

    public void setScrollEnabled(Boolean scrollEnabled) {
        this.scrollEnabled = scrollEnabled;
    }

    public void setScrollTime(Integer scrollTime) {
        this.scrollTime = scrollTime;
    }

    public ArrayEncoding getArrayEncoding() {
        return arrayEncoding;
    }

    public void setArrayEncoding(ArrayEncoding arrayEncoding) {
        this.arrayEncoding = arrayEncoding;
    }

    public Long getGridSize() {
        return gridSize;
    }

    public void setGridSize(Long gridSize) {
        this.gridSize = gridSize;
    }

    public Double getGridThreshold() {
        return gridThreshold;
    }

    public void setGridThreshold(Double gridThreshold) {
        this.gridThreshold = gridThreshold;
    }

    public Map<String, OpenSearchLayerConfiguration> getLayerConfigurations() {
        return layerConfigurations;
    }

    public void setLayerConfiguration(OpenSearchLayerConfiguration layerConfig) {
        final String layerName = layerConfig.getLayerName();
        this.layerConfigurations.put(layerName, layerConfig);
    }

    public Map<Name, String> getDocTypes() {
        return docTypes;
    }

    public String getDocType(Name typeName) {
        final String docType;
        if (docTypes.containsKey(typeName)) {
            docType = docTypes.get(typeName);
        } else {
            docType = typeName.getLocalPart();
        }
        return docType;
    }

    @SuppressWarnings("unchecked")
    private void walk(
            List<OpenSearchAttribute> openSearchAttributes,
            Map<String, Object> map,
            String propertyKey,
            boolean startType,
            boolean nested) {

        for (final Map.Entry<String, Object> entry : map.entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();
            if (!key.equals("_timestamp") && Map.class.isAssignableFrom(value.getClass())) {
                final String newPropertyKey;
                if (!startType && key.equals("properties")) {
                    newPropertyKey = propertyKey;
                } else if (propertyKey.isEmpty()) {
                    newPropertyKey = entry.getKey();
                } else {
                    newPropertyKey = propertyKey + "." + key;
                }
                startType = !startType && key.equals("properties");
                if (!nested && map.containsKey("type")) {
                    nested = map.get("type").equals("nested");
                }

                if (OpenSearchParserUtil.isGeoPointFeature((Map) value)) {
                    add(
                            openSearchAttributes,
                            propertyKey + ".coordinates",
                            "geo_point",
                            (Map) value,
                            nested);
                } else {
                    walk(openSearchAttributes, (Map) value, newPropertyKey, startType, nested);
                }
            } else if (key.equals("type") && !value.equals("nested")) {
                add(openSearchAttributes, propertyKey, (String) value, map, nested);
            } else if (key.equals("_timestamp")) {
                add(openSearchAttributes, "_timestamp", "date", map, nested);
            }
        }
    }

    private void add(
            List<OpenSearchAttribute> openSearchAttributes,
            String propertyKey,
            String propertyType,
            Map<String, Object> map,
            boolean nested) {
        if (propertyKey != null) {
            final OpenSearchAttribute openSearchAttribute = new OpenSearchAttribute(propertyKey);
            final Class<?> binding;
            switch (propertyType) {
                case "geo_point":
                    binding = Point.class;
                    openSearchAttribute.setSrid(4326);
                    openSearchAttribute.setGeometryType(OpenSearchGeometryType.GEO_POINT);
                    break;
                case "geo_shape":
                    binding = Geometry.class;
                    openSearchAttribute.setSrid(4326);
                    openSearchAttribute.setGeometryType(OpenSearchGeometryType.GEO_SHAPE);
                    break;
                case "string":
                case "keyword":
                case "text":
                    binding = String.class;
                    openSearchAttribute.setAnalyzed(isAnalyzed(map));
                    break;
                case "integer":
                    binding = Integer.class;
                    break;
                case "long":
                    binding = Long.class;
                    break;
                case "float":
                    binding = Float.class;
                    break;
                case "double":
                    binding = Double.class;
                    break;
                case "boolean":
                    binding = Boolean.class;
                    break;
                case "date":
                    List<String> validFormats = new ArrayList<>();
                    String availableFormat = (String) map.get("format");
                    if (availableFormat == null) {
                        validFormats.add("date_optional_time");
                    } else {
                        if (!availableFormat.contains("\\|\\|")) {
                            try {
                                ElasticsearchDateConverter.forFormat(availableFormat);
                                validFormats.add(availableFormat);
                            } catch (Exception e) {
                                LOGGER.fine(
                                        "Unable to parse date format ('"
                                                + availableFormat
                                                + "') for "
                                                + propertyKey);
                            }
                        } else {
                            String[] formats = availableFormat.split("\\|\\|");
                            for (String format : formats) {
                                try {
                                    ElasticsearchDateConverter.forFormat(availableFormat);
                                    validFormats.add(format);
                                } catch (Exception e) {
                                    LOGGER.fine(
                                            "Unable to parse date format ('"
                                                    + format
                                                    + "') for "
                                                    + propertyKey);
                                }
                            }
                        }
                    }
                    if (validFormats.isEmpty()) {
                        validFormats.add("date_optional_time");
                    }
                    openSearchAttribute.setValidDateFormats(validFormats);
                    binding = Date.class;
                    break;
                case "binary":
                    binding = byte[].class;
                    break;
                default:
                    binding = null;
                    break;
            }
            if (binding != null) {
                final boolean stored;
                if (map.get("store") != null) {
                    stored = (Boolean) map.get("store");
                } else {
                    stored = false;
                }
                openSearchAttribute.setStored(stored);
                openSearchAttribute.setType(binding);
                openSearchAttribute.setNested(nested);
                openSearchAttributes.add(openSearchAttribute);
            }
        }
    }

    private static void checkRestClient(RestClient client) throws IOException {
        final Response response = client.performRequest(new Request("GET", "/"));
        final int status = response.getStatusLine().getStatusCode();
        if (status >= 400) {
            final String reason = response.getStatusLine().getReasonPhrase();
            throw new IOException(
                    String.format("Unexpected response from OpenSearch: %d %s", status, reason));
        }
    }

    static boolean isAnalyzed(Map<String, Object> map) {
        boolean analyzed = false;
        Object value = map.get("type");
        if (value instanceof String && value.equals("text")) {
            analyzed = true;
        }
        return analyzed;
    }
}
