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

import static org.geotools.filter.visitor.ExtractBoundsFilterVisitor.BOUNDS_VISITOR;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.geotools.data.FeatureReader;
import org.geotools.data.FilteringFeatureReader;
import org.geotools.data.Query;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.opensearch.OpenSearchBucketVisitor;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Envelope;
import org.opengis.feature.FeatureVisitor;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.sort.SortBy;
import org.opengis.filter.sort.SortOrder;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * Provides access to a specific type within the OpenSearch index described by the associated data
 * store.
 */
public class OpenSearchFeatureSource extends ContentFeatureSource {

    private static final Logger LOGGER = Logging.getLogger(OpenSearchFeatureSource.class);

    private Boolean filterFullySupported;

    public OpenSearchFeatureSource(ContentEntry entry, Query query) throws IOException {
        super(entry, query);

        final OpenSearchDataStore dataStore = getDataStore();
        if (dataStore.getLayerConfigurations().get(entry.getName().getLocalPart()) == null) {
            final List<OpenSearchAttribute> attributes =
                    dataStore.getOpenSearchAttributes(entry.getName());
            final OpenSearchLayerConfiguration config =
                    new OpenSearchLayerConfiguration(entry.getName().getLocalPart());
            config.getAttributes().addAll(attributes);
            dataStore.setLayerConfiguration(config);
        }
    }

    /** Access parent datastore */
    @Override
    public OpenSearchDataStore getDataStore() {
        return (OpenSearchDataStore) super.getDataStore();
    }

    /** Implementation that generates the total bounds */
    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        LOGGER.fine("getBoundsInternal");
        final CoordinateReferenceSystem crs = getSchema().getCoordinateReferenceSystem();
        final ReferencedEnvelope bounds = new ReferencedEnvelope(crs);

        try (FeatureReader<SimpleFeatureType, SimpleFeature> featureReader =
                getReaderInternal(query)) {
            while (featureReader.hasNext()) {
                final SimpleFeature feature = featureReader.next();
                bounds.include(feature.getBounds());
            }
        }
        return bounds;
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        LOGGER.fine("getCountInternal");
        int hits = 0;
        final OpenSearchRequest searchRequest = prepareSearchRequest(query, false);
        try {
            if (!filterFullySupported) {
                try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                        getReaderInternal(query)) {
                    while (reader.hasNext()) {
                        reader.next();
                        hits++;
                    }
                }
            } else {
                searchRequest.setSize(0);
                final OpenSearchDataStore dataStore = getDataStore();
                final String docType = dataStore.getDocType(entry.getName());
                final OpenSearchResponse sr =
                        dataStore
                                .getClient()
                                .search(dataStore.getIndexName(), docType, searchRequest);
                final int totalHits = (int) sr.getTotalNumHits();
                final int size = getSize(query);
                final int from = getStartIndex(query);
                hits = Math.max(0, Math.min(totalHits - from, size));
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            throw new IOException("Error executing count search", e);
        }

        return hits;
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
            throws IOException {
        LOGGER.fine("getReaderInternal");
        FeatureReader<SimpleFeatureType, SimpleFeature> reader;
        try {
            final OpenSearchDataStore dataStore = getDataStore();
            final String docType = dataStore.getDocType(entry.getName());
            final boolean scroll =
                    !useSortOrPagination(query)
                            && dataStore.getScrollEnabled()
                            && !isAggregation(query);
            final OpenSearchRequest searchRequest = prepareSearchRequest(query, scroll);
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Search request: " + searchRequest);
            }
            final OpenSearchResponse sr =
                    dataStore.getClient().search(dataStore.getIndexName(), docType, searchRequest);
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Search response: " + sr);
            }
            if (!scroll) {
                reader = new OpenSearchFeatureReader(getState(), sr);
            } else {
                reader = new OpenSearchFeatureReaderScroll(getState(), sr, getSize(query));
            }
            if (!filterFullySupported) {
                reader = new FilteringFeatureReader<>(reader, query.getFilter());
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            throw new IOException("Error executing query search", e);
        }
        return reader;
    }

    private boolean isAggregation(Query query) {
        return query.getHints().get(OpenSearchBucketVisitor.ES_AGGREGATE_BUCKET) != null;
    }

    @Override
    protected boolean handleVisitor(Query query, FeatureVisitor visitor) throws IOException {
        if (visitor instanceof OpenSearchBucketVisitor) {
            OpenSearchBucketVisitor bucketVisitor = (OpenSearchBucketVisitor) visitor;
            final ObjectMapper mapper = new ObjectMapper();
            Map<String, String> hints = new HashMap<>();
            if (bucketVisitor.getAggregationDefinition() != null
                    && bucketVisitor.getAggregationDefinition().length() > 0) {
                hints.put("a", bucketVisitor.getAggregationDefinition());
            }
            if (bucketVisitor.getQueryDefinition() != null
                    && bucketVisitor.getQueryDefinition().length() > 0) {
                hints.put("q", bucketVisitor.getQueryDefinition());
            }

            // if it's Query.ALL passing hints won't work, need to make a modifiable copy
            query = new Query(query);
            query.getHints().put(OpenSearchBucketVisitor.ES_AGGREGATE_BUCKET, hints);
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = getReader(query)) {

                while (reader.hasNext()) {
                    SimpleFeature feature = null;
                    try {
                        feature = reader.next();
                        if (feature.getAttribute("_aggregation") != null) {
                            final byte[] data = (byte[]) feature.getAttribute("_aggregation");
                            final Map<String, Object> aggregation =
                                    mapper.readValue(
                                            data, new TypeReference<Map<String, Object>>() {});
                            bucketVisitor.getBuckets().add(aggregation);
                        }
                    } catch (IOException erp) {
                        LOGGER.fine("Failed to parse aggregation value: " + erp);
                        throw erp;
                    } catch (Exception unexpected) {
                        String fid =
                                feature == null ? "feature" : feature.getIdentifier().toString();
                        throw new IOException(
                                "Problem visiting "
                                        + query.getTypeName()
                                        + " visiting "
                                        + fid
                                        + ":"
                                        + unexpected,
                                unexpected);
                    }
                }
            }

            return true;
        }
        return false;
    }

    private OpenSearchRequest prepareSearchRequest(Query query, boolean scroll) throws IOException {
        String naturalSortOrder = SortOrder.ASCENDING.toSQL().toLowerCase();
        final OpenSearchRequest searchRequest = new OpenSearchRequest();
        final OpenSearchDataStore dataStore = getDataStore();
        final String docType = dataStore.getDocType(entry.getName());

        LOGGER.fine("Preparing " + docType + " (" + entry.getName() + ") query");
        if (!scroll) {
            if (query.getSortBy() != null) {
                for (final SortBy sort : query.getSortBy()) {
                    final String sortOrder = sort.getSortOrder().toSQL().toLowerCase();
                    if (sort.getPropertyName() != null) {
                        final String name = sort.getPropertyName().getPropertyName();
                        searchRequest.addSort(name, sortOrder);
                    } else {
                        naturalSortOrder = sortOrder;
                    }
                }
            }

            // pagination
            searchRequest.setSize(getSize(query));
            searchRequest.setFrom(getStartIndex(query));
        } else {
            if (dataStore.getScrollSize() != null) {
                searchRequest.setSize(dataStore.getScrollSize().intValue());
            }
            if (dataStore.getScrollTime() != null) {
                searchRequest.setScroll(dataStore.getScrollTime());
            }
        }

        if (dataStore.isSourceFilteringEnabled()) {
            if (query.getProperties() != Query.ALL_PROPERTIES) {
                SimpleFeatureType schema = getSchema();
                for (String property : query.getPropertyNames()) {
                    AttributeDescriptor descriptor = schema.getDescriptor(property);
                    if (descriptor != null) {
                        final String sourceName =
                                (String)
                                        descriptor.getUserData().get(OpenSearchConstants.FULL_NAME);
                        searchRequest.addSourceInclude(sourceName);
                    }
                }
            } else {
                // add source includes
                setSourceIncludes(searchRequest);
            }
        }

        // add query and post filter
        final FilterToOpenSearch filterToOpenSearch = new FilterToOpenSearch();
        filterToOpenSearch.setFeatureType(buildFeatureType());
        filterToOpenSearch.encode(query);
        filterFullySupported = filterToOpenSearch.getFullySupported();
        if (!filterFullySupported) {
            LOGGER.fine(
                    "Filter is not fully supported by native OpenSearch."
                            + " Additional post-query filtering will be performed.");
        }
        final Map<String, Object> queryBuilder = filterToOpenSearch.getQueryBuilder();

        final Map<String, Object> nativeQueryBuilder = filterToOpenSearch.getNativeQueryBuilder();

        searchRequest.setQuery(queryBuilder);

        if (isSort(query) && nativeQueryBuilder.equals(OpenSearchConstants.MATCH_ALL)) {
            searchRequest.addSort("_id", naturalSortOrder);
        }

        if (filterToOpenSearch.getAggregations() != null) {
            final Map<String, Map<String, Map<String, Object>>> aggregations =
                    filterToOpenSearch.getAggregations();
            Envelope envelope = getQueryEnvelope(query);
            final long gridSize;
            if (dataStore.getGridSize() != null) {
                gridSize = dataStore.getGridSize();
            } else {
                gridSize = (Long) OpenSearchDataStoreFactory.GRID_SIZE.getDefaultValue();
            }
            final double gridThreshold;
            if (dataStore.getGridThreshold() != null) {
                gridThreshold = dataStore.getGridThreshold();
            } else {
                gridThreshold =
                        (Double) OpenSearchDataStoreFactory.GRID_THRESHOLD.getDefaultValue();
            }
            final int precision = GeohashUtil.computePrecision(envelope, gridSize, gridThreshold);
            GeohashUtil.updateGridAggregationPrecision(aggregations, precision);
            searchRequest.setAggregations(aggregations);
            searchRequest.setSize(0);
        }

        return searchRequest;
    }

    private Envelope getQueryEnvelope(Query query) {
        Envelope envelope = (Envelope) query.getFilter().accept(BOUNDS_VISITOR, null);
        // in case of query not having a spatial filter, assume whole world
        if (Double.isInfinite(envelope.getWidth()))
            envelope = new ReferencedEnvelope(-180, 180, -90, 90, DefaultGeographicCRS.WGS84);
        return envelope;
    }

    private void setSourceIncludes(final OpenSearchRequest searchRequest) throws IOException {
        final OpenSearchDataStore dataStore = getDataStore();
        final List<OpenSearchAttribute> attributes =
                dataStore.getOpenSearchAttributes(entry.getName());
        for (final OpenSearchAttribute attribute : attributes) {
            if (attribute.isUse() && attribute.isStored()) {
                searchRequest.addField(attribute.getName());
            } else if (attribute.isUse()) {
                searchRequest.addSourceInclude(attribute.getName());
            }
        }
    }

    private boolean isSort(Query query) {
        return query.getSortBy() != null && query.getSortBy().length > 0;
    }

    private boolean useSortOrPagination(Query query) {
        return (query.getSortBy() != null && query.getSortBy().length > 0)
                || query.getStartIndex() != null;
    }

    private int getSize(Query query) {
        final int size;
        if (!query.isMaxFeaturesUnlimited()) {
            size = query.getMaxFeatures();
        } else {
            size = getDataStore().getDefaultMaxFeatures();
            LOGGER.fine("Unlimited maxFeatures not supported. Using default: " + size);
        }
        return size;
    }

    private int getStartIndex(Query query) {
        final int from;
        if (query.getStartIndex() != null) {
            from = query.getStartIndex();
        } else {
            from = 0;
        }
        return from;
    }

    @Override
    protected SimpleFeatureType buildFeatureType() {
        final OpenSearchDataStore ds = getDataStore();
        final OpenSearchLayerConfiguration layerConfig =
                ds.getLayerConfigurations().get(entry.getTypeName());
        final List<OpenSearchAttribute> attributes;
        if (layerConfig != null) {
            attributes = layerConfig.getAttributes();
        } else {
            attributes = null;
        }

        final OpenSearchFeatureTypeBuilder typeBuilder =
                new OpenSearchFeatureTypeBuilder(attributes, entry.getName());
        return typeBuilder.buildFeatureType();
    }

    @Override
    protected boolean canLimit() {
        return true;
    }

    @Override
    protected boolean canOffset() {
        return true;
    }

    @Override
    protected boolean canFilter() {
        return true;
    }

    @Override
    protected boolean canSort() {
        return true;
    }
}