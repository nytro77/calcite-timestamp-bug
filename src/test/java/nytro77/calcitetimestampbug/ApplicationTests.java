package nytro77.calcitetimestampbug;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import com.zaxxer.hikari.HikariDataSource;

@SpringBootTest
@TestMethodOrder(OrderAnnotation.class)
class ApplicationTests {

	@DynamicPropertySource
	static void csvdirProperties(DynamicPropertyRegistry registry) {

		File csvDir = null;
		try {
			csvDir = new DefaultResourceLoader().getResource("classpath:TSTAMPS.csv").getFile().getParentFile();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		registry.add("csvdir", csvDir::getAbsolutePath);
	}

	@Autowired
	private AvaticaServer avaticaServer;

	@AfterEach
	public void tearDown() {
		avaticaServer.stop();
	}

	@Test
	public void testTimestamps() {

		try (HikariDataSource ds = new HikariDataSource()) {
			ds.setMaximumPoolSize(1);
			ds.setJdbcUrl(
					"jdbc:avatica:remote:url=http://localhost:" + avaticaServer.getPort() + ";serialization=protobuf;");

			JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);

			List<String> result = jdbcTemplate.queryForList("select THETIMESTAMP from CSV.TSTAMPS", String.class);

			assertEquals(101, result.size());
			assertEquals("1900-01-01 00:00:00", result.get(0));
			assertEquals("1900-01-01 00:00:00", result.get(99));
			assertEquals("1900-01-01 00:00:00", result.get(100)); // Bug will cause this to be 1899-12-31 23:00:00
		}
	}
}
