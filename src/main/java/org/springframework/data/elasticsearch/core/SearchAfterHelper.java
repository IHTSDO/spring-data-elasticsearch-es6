package org.springframework.data.elasticsearch.core;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.data.domain.Pageable;
import org.springframework.util.StringUtils;

public class SearchAfterHelper {

	public static String toSearchAfterToken(final Object[] searchAfter) {
		if (searchAfter == null) {
			return null;
		}
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

	public static <T> String calculateSearchAfterToken(SearchAfterPageRequest pageRequest, T item, Class<T> typeClass) {
		Object[] values = new Object[] {item};
		
		if (!(item instanceof Long)) {
			Method[] sortFieldAccessors = getSortFields(pageRequest, typeClass);
			values = new Object[sortFieldAccessors.length];
			for (int i=0; i<sortFieldAccessors.length; i++) {
				try {
					values[i] = sortFieldAccessors[i].invoke(item);
				} catch (IllegalArgumentException | InvocationTargetException |IllegalAccessException e) {
					throw new IllegalArgumentException("Unable to access " + sortFieldAccessors[i].getName(), e);
				}
			}
		}
		return toSearchAfterToken(values);
	}

	public static <T> Method[] getSortFields(SearchAfterPageRequest pageable, Class<T> typeClass) {
		if (typeClass.equals(Long.class)) {
			try {
				return new Method[] {Long.class.getMethod("toString")};
			} catch (Exception e) {
				throw new IllegalArgumentException("Unable to recover toString on Long");
			} 
		}
		
		return pageable.getSort().stream()
				.map(o -> {
					try {
						return typeClass.getMethod("get" + o.getProperty());
					} catch (Exception e) {
						throw new IllegalArgumentException("Unable to recover search field " + o.getProperty());
					} 
				})
				.collect(Collectors.toList()).toArray(new Method[]{});
	}

	public static <T> void populateSearchAfterToken(List<T> pageOfResults, Pageable pageable, Class<T> typeClass) {
		if (pageable instanceof SearchAfterPageRequest) {
			String searchAfterToken = null;
			//Set our searchAfter token, as long as we've more results to show
			if (pageOfResults.size() > 0) {
				T lastItem = pageOfResults.get(pageOfResults.size()-1);
				searchAfterToken = SearchAfterHelper.calculateSearchAfterToken(((SearchAfterPageRequest)pageable), lastItem, typeClass);
			}
			((SearchAfterPageRequest)pageable).setSearchAfterToken(searchAfterToken);
		}
	}
	
}
