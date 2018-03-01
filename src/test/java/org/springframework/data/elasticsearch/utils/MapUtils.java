package org.springframework.data.elasticsearch.utils;

import java.util.Collections;
import java.util.Map;

public class MapUtils {
	public static String getMapValueUsingCompositeKey(Map map, String compositeKey) {
		String[] keys = compositeKey.split("\\.");
		Object o = map;
		for (String key : keys) {
			o = ((Map)o).getOrDefault(key, Collections.emptyMap());
		}
		return (String) o;
	}
}
