package org.springframework.data.elasticsearch;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class AbstractIntegrationTest {

	private static final String ELASTIC_SEARCH_VERSION = "6.0.1";
	private static final int PORT = 9931;

	private static EmbeddedElastic testElasticsearchSingleton;
	private static File installationDirectory;

	@BeforeClass
	public static void setup() {
		// Share the Elasticsearch instance between test contexts
		if (testElasticsearchSingleton == null) {
			// Create and start a clean standalone Elasticsearch test instance
			String clusterName = "snowstorm-integration-test-cluster";

			try {
				installationDirectory = new File(System.getProperty("java.io.tmpdir"), "embedded-elasticsearch-temp-dir");
				File downloadDir = null;
				if (System.getProperty("user.home") != null) {
					downloadDir = new File(new File(System.getProperty("user.home"), "tmp"), "embedded-elasticsearch-download-cache");
					downloadDir.mkdirs();
				}
				testElasticsearchSingleton = EmbeddedElastic.builder()
						.withElasticVersion(ELASTIC_SEARCH_VERSION)
						.withStartTimeout(30, TimeUnit.SECONDS)
						.withSetting(PopularProperties.CLUSTER_NAME, clusterName)
						.withSetting(PopularProperties.HTTP_PORT, PORT)
						// Manually delete installation directory to prevent verbose error logging
						.withCleanInstallationDirectoryOnStop(false)
						.withDownloadDirectory(downloadDir)
						.withInstallationDirectory(installationDirectory)
						.build();
				testElasticsearchSingleton
						.start()
						.deleteIndices();
			} catch (InterruptedException | IOException e) {
				throw new RuntimeException("Failed to start standalone Elasticsearch instance.", e);
			}
		}
	}

	@AfterClass
	public static void shutdown() {
		synchronized (AbstractIntegrationTest.class) {
			Logger logger = LoggerFactory.getLogger(AbstractIntegrationTest.class);
			if (testElasticsearchSingleton != null) {
				try {
					testElasticsearchSingleton.stop();
				} catch (Exception e) {
					logger.info("The test Elasticsearch instance threw an exception during shutdown, probably due to multiple test contexts. This can be ignored.");
					logger.debug("The test Elasticsearch instance threw an exception during shutdown.", e);
				}
				if (installationDirectory != null && installationDirectory.exists()) {
					try {
						FileUtils.forceDelete(installationDirectory);
					} catch (IOException e) {
						logger.info("Error deleting the test Elasticsearch installation directory from temp {}", installationDirectory.getAbsolutePath());
					}
				}
			}
			testElasticsearchSingleton = null;
		}
	}

}
