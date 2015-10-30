package com.autonavi.odpsup.shp;

import java.io.File;
import java.nio.charset.Charset;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureCollection;
import org.opengis.feature.simple.SimpleFeature;
import org.geotools.feature.FeatureCollection;

public class ShpReader {

	public FeatureCollection getLayerFeatures(String shpPath) {
		try {
			File file = new File(shpPath);
			ShapefileDataStore shpDataStore = new ShapefileDataStore(
					file.toURL());
			shpDataStore.setCharset(Charset.forName("GBK"));
			FeatureSource featureSource = shpDataStore.getFeatureSource();
			System.out.println("图元数目：" + featureSource.getFeatures().size());
			return featureSource.getFeatures();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
