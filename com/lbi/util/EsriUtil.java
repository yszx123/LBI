package com.autonavi.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.FieldSchema;
import com.aliyun.odps.Record;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorBuffer;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorImportFromWkt;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.json.EsriFeature;
import com.esri.json.EsriFeatureClass;
import com.esri.json.EsriField;
import com.esri.json.EsriFieldType;

public class EsriUtil {


	public EsriFeatureClass toEsriFeature(ArrayList<Record> rs){
		SpatialReference spatialReference = SpatialReference.create(3857);
		OperatorImportFromWkt importerWKT = (OperatorImportFromWkt) OperatorFactoryLocal
				.getInstance().getOperator(Operator.Type.ImportFromWkt);
		EsriFeatureClass featureClass = new EsriFeatureClass();
		try {
			// load the wkt file provided as argument 0
			
			featureClass.fields = new EsriField[rs.get(0).getFields().length];
			int c = 0;
			for(FieldSchema fs : rs.get(0).getFields()){
				EsriField f = new EsriField();
				f.alias = fs.name;
				f.name = fs.name;
				f.type = EsriFieldType.esriFieldTypeString;
				featureClass.fields[c] = f;
				c++;
			}
			EsriFeature[] fs = new EsriFeature[rs.size()]; 
			for(int i=0;i<rs.size();i++)
			{
				Polyline polyline = (Polyline) (importerWKT.execute(0, Geometry.Type.Unknown,
						rs.get(i).get("geometry").toString(), null));
				EsriFeature ef = new EsriFeature();
				Map<String, Object> attr = new HashMap<String, Object>();
				OperatorBuffer buffer = (OperatorBuffer) OperatorFactoryLocal
						.getInstance().getOperator(Operator.Type.Buffer);
				Geometry result = buffer.execute(polyline, spatialReference, 1500.0, null);
				Polygon poly = (Polygon) result;
				ef.geometry = poly;
				for(EsriField esriF : featureClass.fields){
					attr.put(esriF.name,rs.get(i).get(esriF.name));
				}
				ef.attributes = attr;
				fs[i] = ef;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			
		}
		return featureClass;
	}
	
}
