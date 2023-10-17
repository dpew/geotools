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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Describes an OpenSearch layer configuration as set of {@link OpenSearchAttribute} */
public class OpenSearchLayerConfiguration implements Serializable {

    private static final long serialVersionUID = 1838874365349725912L;

    /** Key to identify the OpenSearch layer configuration. */
    public static final String KEY = "OpenSearchLayerConfiguration";

    private final String docType;

    private String layerName;

    private final List<OpenSearchAttribute> attributes;

    public OpenSearchLayerConfiguration(String docType) {
        this.docType = docType;
        this.layerName = docType;
        this.attributes = new ArrayList<>();
    }

    public OpenSearchLayerConfiguration(OpenSearchLayerConfiguration other) {
        this(other.docType);
        setLayerName(other.layerName);
        for (final OpenSearchAttribute attribute : other.attributes) {
            attributes.add(new OpenSearchAttribute(attribute));
        }
    }

    public String getDocType() {
        return docType;
    }

    public String getLayerName() {
        return layerName;
    }

    public void setLayerName(String layerName) {
        this.layerName = layerName;
    }

    public List<OpenSearchAttribute> getAttributes() {
        return attributes;
    }
}
