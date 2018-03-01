package org.springframework.data.elasticsearch;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import java.io.IOException;

public class AbstractIntegrationTest {

	private static final String ELASTIC_SEARCH_VERSION = "6.2.0";

	private static EmbeddedElastic embeddedElastic;

	@BeforeClass
	public static void setup() throws IOException, InterruptedException {
		embeddedElastic = EmbeddedElastic.builder()
				.withElasticVersion(ELASTIC_SEARCH_VERSION)
				.withSetting(PopularProperties.CLUSTER_NAME, "integration-test-cluster")
				.withSetting(PopularProperties.HTTP_PORT, 9931)
				.build()
				.start();
		embeddedElastic.deleteIndices();
	}

	@AfterClass
	public static void after() {
		if (embeddedElastic != null) {
			embeddedElastic.stop();
		}
	}

}
