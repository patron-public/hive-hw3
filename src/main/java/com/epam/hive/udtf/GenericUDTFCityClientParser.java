package com.epam.hive.udtf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import com.epam.hive.udtf.OsDetector;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class GenericUDTFCityClientParser extends GenericUDTF {
	private BrowserDetector browserDetector = new BrowserDetector();
	private PrimitiveObjectInspector stringOI = null;
	private PrimitiveObjectInspector intOI = null;

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

		if (args.length != 2) {
			throw new UDFArgumentException("GenericUDTFClientParser() takes exactly one argument");
		}

		if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) args[0])
				.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			throw new UDFArgumentException("GenericUDTFClientParser() takes a string as a first parameter");
		}

		if (args[1].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) args[0])
				.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
			throw new UDFArgumentException("GenericUDTFClientParser() takes a string as a first parameter");
		}

		stringOI = (PrimitiveObjectInspector) args[0];
		intOI = (PrimitiveObjectInspector) args[1];

		List<String> fieldNames = new ArrayList<String>(4);
		List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(4);

		fieldNames.add("os");
		fieldNames.add("browser");
		fieldNames.add("version");
		fieldNames.add("city");

		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaShortObjectInspector);

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	public ArrayList<Object[]> processInputRecord(String clientString, Short city) {
		ArrayList<Object[]> result = new ArrayList<Object[]>();

		// ignoring null or empty input
		if (clientString == null || clientString.isEmpty()) {
			return result;
		}

		result.add(new Object[] { OsDetector.getOsType(clientString),
				browserDetector.getBrowserType(clientString).getKey(),
				browserDetector.getBrowserType(clientString).getValue(), 
				city });

		return result;
	}

	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub
	}

	@Override
	public void process(Object[] record) throws HiveException {
		final String name = stringOI.getPrimitiveJavaObject(record[0]).toString();
		final Short city = (Short) intOI.getPrimitiveJavaObject(record[1]);

		ArrayList<Object[]> results = processInputRecord(name, city);

		Iterator<Object[]> it = results.iterator();

		while (it.hasNext()) {
			Object[] r = it.next();
			forward(r);
		}
	}

}
