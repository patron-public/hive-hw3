package com.epam.hive.udtf;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BrowserDetector {
	public BrowserDetector() {
		init();
	}

	private static final String NA = "N/A";

	public enum BROWSER_TYPE {
		WA360, IE, CHROME, MAXTHON, SAFARI, OTHER
	}

	private List<BrowserDetectionInfo> detectionInfos = new ArrayList<BrowserDetectionInfo>();


	private void init() {
		detectionInfos.add(new BrowserDetectionInfo("(.*)(MSIE)(\\s)(\\d{1,2})(\\.\\d.*)", 4, BROWSER_TYPE.IE));
		detectionInfos.add(new BrowserDetectionInfo("(.*)(Chrome)(/)(\\d{1,2})(\\..*)", 4, BROWSER_TYPE.CHROME));
		detectionInfos.add(new BrowserDetectionInfo("(.*)(Safari)(/)(\\d{1,4})(\\..*)", 4, BROWSER_TYPE.SAFARI));
	}

	public Map.Entry<String, String> getBrowserType(String clientString) {
		for (BrowserDetectionInfo info : detectionInfos) {
			
			if (info.matches(clientString)) {
				String browser = info.getBrowserType().name();
				String version = info.getVersion();
				return new AbstractMap.SimpleEntry<String, String>(browser, version);
			}
		}
		return new AbstractMap.SimpleEntry<String, String>(BROWSER_TYPE.OTHER.name(), NA);
	}

	public class BrowserDetectionInfo {
		private BROWSER_TYPE browserType;
		private String version;
		private String regexp;
		private int groupNo;

		public BrowserDetectionInfo(String regexp, int groupNo, BROWSER_TYPE browserType) {
			this.browserType = browserType;
			this.regexp = regexp;
			this.groupNo = groupNo;
		}

		public boolean matches(String clientString) {
			Pattern r = Pattern.compile(regexp);
			Matcher m = r.matcher(clientString);

			if (m.find()) {
				version = m.group(groupNo);
				return true;
			}

			return false;
		}

		public BROWSER_TYPE getBrowserType() {
			return browserType;
		}

		public String getVersion() {
			return version;
		}
	}
}
