package com.epam.hive.udtf;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.epam.hive.udtf.BrowserDetector.BROWSER_TYPE;

public class BrowserDetectorTest {

	BrowserDetector detector;

	@Before
	public void initDetector() {
		detector = new BrowserDetector();
	}

	@Test
	public void chromeTest() {
		String line = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1";
		Map.Entry<String, String> detectionResult = detector.getBrowserType(line);
		assertEquals(BROWSER_TYPE.CHROME.name(),detectionResult.getKey());
		assertEquals("21",detectionResult.getValue());
	}
	
	@Test
	public void ieTest() {
		String line = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)";
		Map.Entry<String, String> detectionResult = detector.getBrowserType(line);
		assertEquals(BROWSER_TYPE.IE.name(),detectionResult.getKey());
		assertEquals("8",detectionResult.getValue());
	}
	
	@Test
	public void safariTest() {
		String line = "Mozilla/5.0 (iPad; CPU OS 6_1_2 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A406 Safari/8536.25";
		Map.Entry<String, String> detectionResult = detector.getBrowserType(line);
		assertEquals(BROWSER_TYPE.SAFARI.name(),detectionResult.getKey());
		assertEquals("8536",detectionResult.getValue());
	}
}
