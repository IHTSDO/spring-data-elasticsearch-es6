package org.springframework.data.elasticsearch.core;

import java.io.*;
import java.util.Base64;
import java.util.List;

import org.springframework.util.StringUtils;

public class SearchAfterHelper {

	public static String toSearchAfterToken(final Object[] searchAfter) {
		try {
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			ObjectOutputStream so = new ObjectOutputStream(bo);
			so.writeObject(searchAfter);
			so.flush();
			return new String(Base64.getUrlEncoder().encode(bo.toByteArray()));
		} catch (Exception e) {
			throw new IllegalArgumentException("Failed to serialize 'searchAfter' array", e);
		}
	}

	public static Object[] fromSearchAfterToken(final String searchAfterToken) {
		if (StringUtils.isEmpty(searchAfterToken)) {
			return null;
		}
		
		try {
			byte b[] = Base64.getUrlDecoder().decode(searchAfterToken.getBytes()); 
			ByteArrayInputStream bi = new ByteArrayInputStream(b);
			ObjectInputStream si = new ObjectInputStream(bi);
			return (Object[])si.readObject();
		} catch (Exception e) {
			throw new IllegalArgumentException("Failed to deserialize 'searchAfter' token", e);
		}
	}
	
}
