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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.http.HttpHost;
import org.geotools.data.DataStore;
import org.junit.Test;
import org.opensearch.client.Node;

public class OpenSearchDataStoreFinderIT extends OpenSearchTestSupport {

    @Test
    public void testFactoryDefaults() throws IOException {
        Map<String, Serializable> params = createConnectionParams();
        OpenSearchDataStoreFactory factory = new OpenSearchDataStoreFactory();
        dataStore = (OpenSearchDataStore) factory.createDataStore(params);

        OpenSearchDataStoreFactory fac = new OpenSearchDataStoreFactory();
        assertEquals(fac.getDisplayName(), OpenSearchDataStoreFactory.DISPLAY_NAME);
        assertEquals(fac.getDescription(), OpenSearchDataStoreFactory.DESCRIPTION);
        assertArrayEquals(fac.getParametersInfo(), OpenSearchDataStoreFactory.PARAMS);
        assertNull(fac.getImplementationHints());
        assertNull(fac.createNewDataStore(null));
    }

    @Test
    public void testFactory() throws IOException {
        OpenSearchDataStoreFactory factory = new OpenSearchDataStoreFactory();
        assertTrue(factory.isAvailable());

        Map<String, Serializable> map = new HashMap<>();
        map.put(OpenSearchDataStoreFactory.HOSTNAME.key, "localhost");
        map.put(OpenSearchDataStoreFactory.HOSTPORT.key, port);
        map.put(OpenSearchDataStoreFactory.INDEX_NAME.key, "sample");

        DataStore store = factory.createDataStore(map);

        assertNotNull(store);
        assertTrue(store instanceof OpenSearchDataStore);
    }

    @Test
    public void testFactoryWithMissingRequired() throws IOException {
        OpenSearchDataStoreFactory factory = new OpenSearchDataStoreFactory();
        assertTrue(factory.isAvailable());

        assertFalse(
                factory.canProcess(
                        ImmutableMap.of(
                                OpenSearchDataStoreFactory.HOSTNAME.key,
                                "localhost",
                                OpenSearchDataStoreFactory.HOSTPORT.key,
                                port)));
        assertFalse(
                factory.canProcess(
                        ImmutableMap.of(
                                OpenSearchDataStoreFactory.HOSTNAME.key,
                                "localhost",
                                OpenSearchDataStoreFactory.INDEX_NAME.key,
                                "test")));
        assertFalse(
                factory.canProcess(
                        ImmutableMap.of(OpenSearchDataStoreFactory.HOSTNAME.key, "localhost")));
        assertFalse(
                factory.canProcess(
                        ImmutableMap.of(
                                OpenSearchDataStoreFactory.HOSTPORT.key,
                                port,
                                OpenSearchDataStoreFactory.INDEX_NAME.key,
                                "test")));
        assertFalse(
                factory.canProcess(ImmutableMap.of(OpenSearchDataStoreFactory.HOSTPORT.key, port)));
        assertFalse(
                factory.canProcess(
                        ImmutableMap.of(OpenSearchDataStoreFactory.INDEX_NAME.key, "test")));
    }

    @Test
    public void testCreateRestClient() throws IOException {
        assertEquals(
                ImmutableList.of(new HttpHost("localhost", port, "http")), getHosts("localhost"));
        assertEquals(
                ImmutableList.of(new HttpHost("localhost.localdomain", port, "http")),
                getHosts("localhost.localdomain"));

        assertEquals(
                ImmutableList.of(new HttpHost("localhost", 9201, "http")),
                getHosts("localhost:9201"));
        assertEquals(
                ImmutableList.of(new HttpHost("localhost.localdomain", 9201, "http")),
                getHosts("localhost.localdomain:9201"));

        assertEquals(
                ImmutableList.of(new HttpHost("localhost", port, "http")),
                getHosts("http://localhost"));
        assertEquals(
                ImmutableList.of(new HttpHost("localhost", 9200, "http")),
                getHosts("http://localhost:9200"));
        assertEquals(
                ImmutableList.of(new HttpHost("localhost", 9201, "http")),
                getHosts("http://localhost:9201"));

        assertEquals(
                ImmutableList.of(new HttpHost("localhost", port, "https")),
                getHosts("https://localhost"));
        assertEquals(
                ImmutableList.of(new HttpHost("localhost", 9200, "https")),
                getHosts("https://localhost:9200"));
        assertEquals(
                ImmutableList.of(new HttpHost("localhost", 9201, "https")),
                getHosts("https://localhost:9201"));

        assertEquals(
                ImmutableList.of(
                        new HttpHost("somehost.somedomain", port, "http"),
                        new HttpHost("anotherhost.somedomain", port, "http")),
                getHosts("somehost.somedomain:" + port + ",anotherhost.somedomain:" + port));
        assertEquals(
                ImmutableList.of(
                        new HttpHost("somehost.somedomain", port, "https"),
                        new HttpHost("anotherhost.somedomain", port, "https")),
                getHosts(
                        "https://somehost.somedomain:"
                                + port
                                + ",https://anotherhost.somedomain:"
                                + port));
        assertEquals(
                ImmutableList.of(
                        new HttpHost("somehost.somedomain", port, "https"),
                        new HttpHost("anotherhost.somedomain", port, "https")),
                getHosts(
                        "https://somehost.somedomain:"
                                + port
                                + ", https://anotherhost.somedomain:"
                                + port));
        assertEquals(
                ImmutableList.of(
                        new HttpHost("somehost.somedomain", port, "https"),
                        new HttpHost("anotherhost.somedomain", port, "http")),
                getHosts(
                        "https://somehost.somedomain:" + port + ",anotherhost.somedomain:" + port));
    }

    private List<HttpHost> getHosts(String hosts) throws IOException {
        Map<String, Serializable> params = createConnectionParams();
        params.put(OpenSearchDataStoreFactory.HOSTNAME.key, hosts);
        OpenSearchDataStoreFactory factory = new OpenSearchDataStoreFactory();
        return factory.createRestClient(params).getNodes().stream()
                .map(Node::getHost)
                .collect(Collectors.toList());
    }
}
