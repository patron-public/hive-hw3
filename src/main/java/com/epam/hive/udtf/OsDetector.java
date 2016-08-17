package com.epam.hive.udtf;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OsDetector {

	public enum OS_TYPE {
		WA360, WINDOWS_8, WINDOWS_7, WINDOWS_XP, WINDOWS_VISTA, WINDOWS_2000, WINDOWS_NT, LINUX, UBUNTU, MAC_OS_X, OTHER
	}

	private static Map<OS_TYPE, Set<String>> osMap = new HashMap<OS_TYPE, Set<String>>();

	static {
		osMap.put(OS_TYPE.WA360, new HashSet<String>(Arrays.asList("360WebApp")));
		osMap.put(OS_TYPE.WINDOWS_8, new HashSet<String>(Arrays.asList("Windows NT 6.2")));
		osMap.put(OS_TYPE.WINDOWS_7, new HashSet<String>(Arrays.asList("Windows NT 6.1")));
		osMap.put(OS_TYPE.WINDOWS_VISTA, new HashSet<String>(Arrays.asList("Windows NT 6.0")));
		osMap.put(OS_TYPE.WINDOWS_XP, new HashSet<String>(Arrays.asList("Windows NT 5.1")));
		osMap.put(OS_TYPE.WINDOWS_2000, new HashSet<String>(Arrays.asList("Windows NT 5.0")));
		osMap.put(OS_TYPE.WINDOWS_NT, new HashSet<String>(Arrays.asList("Windows NT 4.")));
		osMap.put(OS_TYPE.MAC_OS_X, new HashSet<String>(Arrays.asList("Mac OS X")));
		osMap.put(OS_TYPE.UBUNTU, new HashSet<String>(Arrays.asList("X11; Ubuntu")));
		osMap.put(OS_TYPE.LINUX, new HashSet<String>(Arrays.asList("X11; Linux")));
	}

	public static String getOsType(String str) {
		for (OS_TYPE key : osMap.keySet()) {
			for (String cond : osMap.get(key))
				if (str.contains(cond)) {
					return key.name();
				}
		}
		return OS_TYPE.OTHER.name();
	}
}
