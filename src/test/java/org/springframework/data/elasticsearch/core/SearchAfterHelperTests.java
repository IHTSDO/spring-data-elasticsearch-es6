package org.springframework.data.elasticsearch.core;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class SearchAfterHelperTests {
	
	String FOO = "foo";
	String BAR = "bar";
	String [] testArray;
	String expectedToken = "rO0ABXVyABNbTGphdmEubGFuZy5TdHJpbmc7rdJW5-kde0cCAAB4cAAAAAJ0AANmb290AANiYXI=";
	
	@Before 
	public void before() {
		testArray = new String[] { FOO, BAR };
	}

	@Test
	public void toSearchAfterToken() {
		String token = SearchAfterHelper.toSearchAfterToken(testArray);
		assertEquals(expectedToken, token);
	}
	
	@Test
	public void fromSearchAfterToken() {
		Object[] arr = SearchAfterHelper.fromSearchAfterToken(expectedToken);
		assertEquals(FOO, arr[0]);
		assertEquals(BAR, arr[1]);
	}
}
