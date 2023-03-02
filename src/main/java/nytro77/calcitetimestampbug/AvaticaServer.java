package nytro77.calcitetimestampbug;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;

import javax.annotation.PreDestroy;

import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.stereotype.Component;

@Component
public class AvaticaServer implements ApplicationRunner {

	private static final Logger LOG = LoggerFactory.getLogger(AvaticaServer.class);

	private HttpServer server;

	@Value("${avatica.server.port}")
	private int port;

	@Value("${csvdir}")
	private String csvdir;

	@Value("${testmode}")
	private boolean testmode;

	private void buildAndStart() {
		try {

			// Build Calcite model URL
			//
			String model = null;
			try (InputStream is = new DefaultResourceLoader().getResource("classpath:model-template.txt")
					.getInputStream()) {
				model = new String(is.readAllBytes(), StandardCharsets.UTF_8).replace("${csvdir}",
						csvdir.replace("\\", "\\\\"));
			}

			JdbcMeta meta = new JdbcMeta("jdbc:calcite:model=inline:" + model);

			LocalService service = new LocalService(meta);

			LOG.info("Starting Avatica server on {}", (port == 0 ? "random port" : "port " + port));

			server = new HttpServer.Builder<HttpServer>().withHandler(service, Serialization.PROTOBUF).withPort(port)
					.build();

			server.start();
			port = server.getPort();

			LOG.info("Avatica server is up and listening on port {}", port);

			LOG.info("Getting Calcite connection and fething tables from CSV files");
			try (Connection connection = DriverManager.getConnection(
					"jdbc:avatica:remote:url=http://localhost:" + port + ";serialization=protobuf;", null, null)) {

				DatabaseMetaData databaseMetaData = connection.getMetaData();
				try (ResultSet resultSet = databaseMetaData.getTables(null, "CSV", "%", null)) {
					int i = 0;
					while (resultSet.next()) {
						LOG.info("Table {}", resultSet.getString("TABLE_NAME"));
						++i;
					}

					LOG.info("Fetched {} tables", i);
				}
			}

		} catch (Exception e) {
			LOG.error("Failed to start Avatica server. Error: " + e.getMessage(), e);
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			}
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {

		buildAndStart();

		if (!testmode) {
			LOG.info("Server waiting for calls...");
			server.join();
		}
	}

	@PreDestroy
	public void stop() {
		if (server != null) {
			LOG.info("Stopping server");
			server.stop();
			server = null;
			LOG.info("Server stopped");
		}
	}

	public int getPort() {
		return port;
	}

}
