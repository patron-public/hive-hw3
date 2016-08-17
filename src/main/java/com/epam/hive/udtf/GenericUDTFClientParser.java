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

public class GenericUDTFClientParser extends GenericUDTF {
	private BrowserDetector browserDetector = new BrowserDetector();
	private PrimitiveObjectInspector stringOI = null;

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

		if (args.length != 1) {
			throw new UDFArgumentException("GenericUDTFClientParser() takes exactly one argument");
		}

		if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) args[0])
				.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			throw new UDFArgumentException("GenericUDTFClientParser() takes a string as a parameter");
		}

		stringOI = (PrimitiveObjectInspector) args[0];

		List<String> fieldNames = new ArrayList<String>(3);
		List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(3);
		
		fieldNames.add("os");
		fieldNames.add("browser");
		fieldNames.add("version");

		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	public ArrayList<Object[]> processInputRecord(String clientString) {
		ArrayList<Object[]> result = new ArrayList<Object[]>();

		// ignoring null or empty input
		if (clientString == null || clientString.isEmpty()) {
			return result;
		}

		result.add(new Object[] { OsDetector.getOsType(clientString),
				browserDetector.getBrowserType(clientString).getKey(),
				browserDetector.getBrowserType(clientString).getValue() });

		return result;
	}

	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub
	}

	@Override
	public void process(Object[] record) throws HiveException {
		final String name = stringOI.getPrimitiveJavaObject(record[0]).toString();

		ArrayList<Object[]> results = processInputRecord(name);

		Iterator<Object[]> it = results.iterator();

		while (it.hasNext()) {
			Object[] r = it.next();
			forward(r);
		}
	}

}
