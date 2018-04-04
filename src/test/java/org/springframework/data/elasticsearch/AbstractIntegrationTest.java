package org.springframework.data.elasticsearch;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class AbstractIntegrationTest {

	private static final String ELASTIC_SEARCH_VERSION = "6.0.1";

	private static EmbeddedElastic embeddedElastic;

	@BeforeClass
	public static void setup() throws IOException, InterruptedException {
		embeddedElastic = EmbeddedElastic.builder()
				.withElasticVersion(ELASTIC_SEARCH_VERSION)
				.withStartTimeout(30, TimeUnit.SECONDS)
				.withSetting(PopularProperties.CLUSTER_NAME, "integration-test-cluster")
				.withSetting("rest.action.multi.allow_explicit_index", "false")
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
