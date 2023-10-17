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

import static org.geotools.data.opensearch.OpenSearchConstants.ANALYZED;
import static org.geotools.data.opensearch.OpenSearchConstants.DATE_FORMAT;
import static org.geotools.data.opensearch.OpenSearchConstants.FULL_NAME;
import static org.geotools.data.opensearch.OpenSearchConstants.GEOMETRY_TYPE;
import static org.geotools.data.opensearch.OpenSearchConstants.NESTED;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.Name;

/**
 * Builds a feature type based on the attributes defined in the {@link
 * OpenSearchLayerConfiguration}.
 */
class OpenSearchFeatureTypeBuilder extends SimpleFeatureTypeBuilder {

    private static final Logger LOGGER = Logging.getLogger(OpenSearchFeatureTypeBuilder.class);

    private final List<OpenSearchAttribute> attributes;

    public OpenSearchFeatureTypeBuilder(List<OpenSearchAttribute> attributes, Name name) {
        setName(name);
        this.attributes = attributes;
    }

    @Override
    public SimpleFeatureType buildFeatureType() {
        if (attributes != null) {
            String defaultGeometryName = null;
            for (OpenSearchAttribute attribute : attributes) {
                if (attribute.isUse()) {
                    final String attributeName;
                    if (attribute.getCustomName() != null) {
                        attributeName = attribute.getCustomName();
                    } else {
                        attributeName = attribute.getName();
                    }

                    AttributeDescriptor att = null;
                    if (Geometry.class.isAssignableFrom(attribute.getType())) {
                        final Integer srid = attribute.getSrid();
                        try {
                            if (srid != null) {
                                attributeBuilder.setCRS(CRS.decode("EPSG:" + srid));
                                attributeBuilder.setName(attributeName);
                                attributeBuilder.setBinding(attribute.getType());
                                att =
                                        attributeBuilder.buildDescriptor(
                                                attributeName,
                                                attributeBuilder.buildGeometryType());

                                final OpenSearchAttribute.OpenSearchGeometryType geometryType =
                                        attribute.getGeometryType();
                                att.getUserData().put(GEOMETRY_TYPE, geometryType);
                                if (attribute.isDefaultGeometry() != null
                                        && attribute.isDefaultGeometry()) {
                                    defaultGeometryName = attributeName;
                                }
                            }
                        } catch (Exception e) {
                            String msg = "Error occured determing srid for " + attribute.getName();
                            LOGGER.log(Level.WARNING, msg, e);
                        }
                    } else {
                        attributeBuilder.setName(attributeName);
                        attributeBuilder.setBinding(attribute.getType());
                        att =
                                attributeBuilder.buildDescriptor(
                                        attributeName, attributeBuilder.buildType());
                    }
                    if (att != null
                            && (attribute.getValidDateFormats() != null
                                    || attribute.getDateFormat() != null)) {
                        if (attribute.getValidDateFormats() == null) {
                            List<String> validFormats = new ArrayList<>();
                            validFormats.add(attribute.getDateFormat());
                            attribute.setValidDateFormats(validFormats);
                        }
                        att.getUserData().put(DATE_FORMAT, attribute.getValidDateFormats());
                    }
                    if (att != null) {
                        att.getUserData().put(FULL_NAME, attribute.getName());
                        att.getUserData().put(ANALYZED, attribute.getAnalyzed());
                        att.getUserData().put(NESTED, attribute.isNested());
                        add(att);
                    }
                }
            }
            if (defaultGeometryName != null) {
                setDefaultGeometry(defaultGeometryName);
            }
        }
        return super.buildFeatureType();
    }
}
