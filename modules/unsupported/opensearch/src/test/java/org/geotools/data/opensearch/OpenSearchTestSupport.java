/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2014, Open Source Geospatial Foundation (OSGeo)
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TimeZone;
import org.apache.http.HttpHost;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.NameImpl;
import org.geotools.temporal.object.DefaultInstant;
import org.geotools.temporal.object.DefaultPeriod;
import org.geotools.temporal.object.DefaultPosition;
import org.junit.After;
import org.junit.Before;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.temporal.Instant;
import org.opengis.temporal.Period;
import org.opensearch.client.RestClient;
import org.opensearch.testcontainers.OpensearchContainer;

public class OpenSearchTestSupport {

    private static final String IMAGE_PROPERTY_NAME = "opensearch.test.image";

    /** The pure Apache licensed version */
    private static final String DEFAULT_IMAGE = "opensearchproject/opensearch";

    private static final String VERSION_PROPERTY_NAME = "opensearch.test.version";

    /** Last version provided on the OSS build */
    private static final String DEFAULT_VERSION = "2.9.0";

    private static OpensearchContainer opensearch;

    static {
        String image = System.getProperty(IMAGE_PROPERTY_NAME, DEFAULT_IMAGE);
        String version = System.getProperty(VERSION_PROPERTY_NAME, DEFAULT_VERSION);
        opensearch = new OpensearchContainer(image + ":" + version);
        opensearch.start();
    }

    private static final String TEST_FILE = "wifiAccessPoint.json";

    private static final String ACTIVE_MAPPINGS_FILE = "active_mappings.json";

    private static final int numShards = 1;

    private static final int numReplicas = 0;

    private static final boolean SCROLL_ENABLED = false;

    private static final long SCROLL_SIZE = 20;

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final ObjectReader mapReader =
            mapper.readerWithView(Map.class).forType(HashMap.class);

    static final String TYPE_NAME = "active";

    static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-dd-MM HH:mm:ss");

    static final int SOURCE_SRID = 4326;

    protected String host;

    protected int port;

    protected String indexName;

    protected OpenSearchDataStore dataStore;

    protected OpenSearchFeatureSource featureSource;

    protected OpenSearchLayerConfiguration config;

    protected OpenSearchClient client;

    @Before
    public void beforeTest() throws Exception {
        host = opensearch.getContainerIpAddress();
        port = opensearch.getFirstMappedPort();
        indexName = "gt_integ_test_" + System.nanoTime();
        client =
                new RestOpenSearchClient(
                        RestClient.builder(new HttpHost(host, port, "http")).build());
        Map<String, Serializable> params = createConnectionParams();
        OpenSearchDataStoreFactory factory = new OpenSearchDataStoreFactory();
        dataStore = (OpenSearchDataStore) factory.createDataStore(params);
        createIndices();
    }

    @After
    public void afterTest() throws Exception {
        performRequest("DELETE", "/" + indexName, null);
        dataStore.dispose();
        client.close();
    }

    private void createIndices() throws IOException {
        // create index and add mappings
        Map<String, Object> settings = new HashMap<>();
        settings.put(
                "settings",
                ImmutableMap.of("number_of_shards", numShards, "number_of_replicas", numReplicas));
        final String filename = ACTIVE_MAPPINGS_FILE;
        final InputStream resource = ClassLoader.getSystemResourceAsStream(filename);
        if (resource != null) {
            try (Scanner s = new Scanner(resource)) {
                s.useDelimiter("\\A");
                Map<String, Object> source = mapReader.readValue(s.next());
                settings.put("mappings", source);
            }
        }
        performRequest("PUT", "/" + indexName, settings);

        // add alias
        Map<String, Object> aliases =
                ImmutableMap.of(
                        "actions",
                        ImmutableList.of(
                                ImmutableMap.of(
                                        "index", indexName, "alias", indexName + "_alias")));
        performRequest("PUT", "/_alias", aliases);
    }

    private void indexDocuments(String status) throws IOException {
        try (InputStream inputStream = ClassLoader.getSystemResourceAsStream(TEST_FILE)) {
            if (inputStream != null) {
                try (Scanner scanner = new Scanner(inputStream)) {
                    scanner.useDelimiter(System.lineSeparator());
                    final StringBuilder builder = new StringBuilder();
                    while (scanner.hasNext()) {
                        final String line = scanner.next();
                        if (!line.startsWith("#")) {
                            builder.append(line);
                        }
                    }
                    final Map<String, Object> content = mapReader.readValue(builder.toString());
                    @SuppressWarnings("unchecked")
                    final List<Map<String, Object>> features =
                            (List<Map<String, Object>>) content.get("features");
                    for (final Map<String, Object> featureSource : features) {
                        if (featureSource.containsKey("status_s")
                                && featureSource.get("status_s").equals(status)) {
                            final String id =
                                    featureSource.containsKey("id")
                                            ? (String) featureSource.get("id")
                                            : null;
                            final String typeName = "_doc";
                            performRequest(
                                    "POST",
                                    "/" + indexName + "/" + typeName + "/" + id,
                                    featureSource);
                        }
                    }

                    performRequest("POST", "/" + indexName + "/_refresh", null);
                }
            }
        }
    }

    Map<String, Serializable> createConnectionParams() {
        Map<String, Serializable> params = new HashMap<>();
        params.put(OpenSearchDataStoreFactory.HOSTNAME.key, host);
        params.put(OpenSearchDataStoreFactory.HOSTPORT.key, port);
        params.put(OpenSearchDataStoreFactory.INDEX_NAME.key, indexName);
        params.put(OpenSearchDataStoreFactory.SCROLL_ENABLED.key, SCROLL_ENABLED);
        params.put(OpenSearchDataStoreFactory.SCROLL_SIZE.key, SCROLL_SIZE);
        return params;
    }

    protected void init() throws Exception {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        init("active");
    }

    void init(String layerName) throws Exception {
        init(layerName, "geo");
    }

    void init(String status, String geometryField) throws Exception {
        indexDocuments(status);
        List<OpenSearchAttribute> attributes =
                dataStore.getOpenSearchAttributes(new NameImpl(TYPE_NAME));
        config = new OpenSearchLayerConfiguration(TYPE_NAME);
        List<OpenSearchAttribute> layerAttributes = new ArrayList<>();
        for (OpenSearchAttribute attribute : attributes) {
            attribute.setUse(true);
            if (geometryField.equals(attribute.getName())) {
                OpenSearchAttribute copy = new OpenSearchAttribute(attribute);
                copy.setDefaultGeometry(true);
                layerAttributes.add(copy);
            } else {
                layerAttributes.add(attribute);
            }
        }
        config.getAttributes().clear();
        config.getAttributes().addAll(layerAttributes);
        dataStore.setLayerConfiguration(config);
        featureSource = (OpenSearchFeatureSource) dataStore.getFeatureSource(TYPE_NAME);
    }

    private void performRequest(String method, String endpoint, Map<String, Object> body)
            throws IOException {
        ((RestOpenSearchClient) client).performRequest(method, endpoint, body);
    }

    private Date date(String date) throws ParseException {
        return DATE_FORMAT.parse(date);
    }

    private Instant instant(String d) throws ParseException {
        return new DefaultInstant(new DefaultPosition(date(d)));
    }

    Period period(String d1, String d2) throws ParseException {
        return new DefaultPeriod(instant(d1), instant(d2));
    }

    List<SimpleFeature> readFeatures(SimpleFeatureIterator iterator) {
        final List<SimpleFeature> features = new ArrayList<>();
        try {
            while (iterator.hasNext()) {
                features.add(iterator.next());
            }
        } finally {
            iterator.close();
        }
        return features;
    }
}
