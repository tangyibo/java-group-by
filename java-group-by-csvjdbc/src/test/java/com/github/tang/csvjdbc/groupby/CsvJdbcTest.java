package com.github.tang.csvjdbc.groupby;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Before;
import org.junit.Test;

public class CsvJdbcTest {

	@Before
	public void init() {
		System.out.println("================================CsvJdbc======================================");
	}

	@Test
	public void testGroupByUseCsvFile() throws SQLException {
		Properties props = new Properties();
		props.put("headerline", "a,b");
		props.put("suppressHeaders", "false");
		props.put("fileExtension", ".csv");
		props.put("commentChar", "#");
		props.put("columnTypes", "Integer,Integer");

		String path = CsvJdbcTest.class.getResource("/").toString();
		path = path.substring(5);

		String sql = "select avg(a) as \"avg_a\",b from test group by b";

		StopWatch watch = new StopWatch();
		watch.start();

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + path, props);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		ResultSetMetaData rsmd = rs.getMetaData();
		while (rs.next()) {
			Map<String, Object> row = new HashMap<>();
			for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
				row.put(rsmd.getColumnName(i), rs.getObject(i));
			}

			System.out.println(row);
		}

		watch.stop();
		System.out.println("Test Execute Group by Time Elapsed: " + watch.getTime());
	}

	@Test
	public void testGroupByUseCsvJdbcFoo100w() throws SQLException {
		Properties props = new Properties();
		props.put("headerline", "a,b");
		props.put("suppressHeaders", "false");
		props.put("fileExtension", ".csv");
		props.put("commentChar", "#");
		props.put("columnTypes", "Integer,Integer");

		String path = CsvJdbcTest.class.getResource("/").toString();
		path = path.substring(5);

		String sql = "select avg(a) as \"avg_a\",b from foo100 group by b";

		StopWatch watch = new StopWatch();
		watch.start();

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + path, props);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		ResultSetMetaData rsmd = rs.getMetaData();
		while (rs.next()) {
			Map<String, Object> row = new HashMap<>();
			for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
				row.put(rsmd.getColumnName(i), rs.getObject(i));
			}

			System.out.println(row);
		}

		watch.stop();
		System.out.println("Test Execute Big Data 100w Group by Time Elapsed: " + watch.getTime());
	}

	@Test
	public void testGroupByUseCsvJdbcFoo500w() throws SQLException {
		Properties props = new Properties();
		props.put("headerline", "a,b");
		props.put("suppressHeaders", "false");
		props.put("fileExtension", ".csv");
		props.put("commentChar", "#");
		props.put("columnTypes", "Integer,Integer");

		String path = CsvJdbcTest.class.getResource("/").toString();
		path = path.substring(5);

		String sql = "select avg(a) as \"avg_a\",b from foo500 group by b";

		StopWatch watch = new StopWatch();
		watch.start();

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + path, props);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		ResultSetMetaData rsmd = rs.getMetaData();
		while (rs.next()) {
			Map<String, Object> row = new HashMap<>();
			for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
				row.put(rsmd.getColumnName(i), rs.getObject(i));
			}

			System.out.println(row);
		}

		watch.stop();
		System.out.println("Test Execute Big Data 500w Group by Time Elapsed: " + watch.getTime());
	}

	@Test
	public void testGroupByUseCsvJdbcFoo600w() throws SQLException {
		Properties props = new Properties();
		props.put("headerline", "a,b");
		props.put("suppressHeaders", "false");
		props.put("fileExtension", ".csv");
		props.put("commentChar", "#");
		props.put("columnTypes", "Integer,Integer");

		String path = CsvJdbcTest.class.getResource("/").toString();
		path = path.substring(5);

		String sql = "select avg(a) as \"avg_a\",b from foo600 group by b";

		StopWatch watch = new StopWatch();
		watch.start();

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + path, props);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		ResultSetMetaData rsmd = rs.getMetaData();
		while (rs.next()) {
			Map<String, Object> row = new HashMap<>();
			for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
				row.put(rsmd.getColumnName(i), rs.getObject(i));
			}

			System.out.println(row);
		}

		watch.stop();
		System.out.println("Test Execute Big Data 600w Group by Time Elapsed: " + watch.getTime());
	}

	@Test
	public void testGroupByUseCsvJdbcFoo700w() throws SQLException {
		Properties props = new Properties();
		props.put("headerline", "a,b");
		props.put("suppressHeaders", "false");
		props.put("fileExtension", ".csv");
		props.put("commentChar", "#");
		props.put("columnTypes", "Integer,Integer");

		String path = CsvJdbcTest.class.getResource("/").toString();
		path = path.substring(5);

		String sql = "select avg(a) as \"avg_a\",b from foo700 group by b";

		StopWatch watch = new StopWatch();
		watch.start();

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + path, props);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		ResultSetMetaData rsmd = rs.getMetaData();
		while (rs.next()) {
			Map<String, Object> row = new HashMap<>();
			for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
				row.put(rsmd.getColumnName(i), rs.getObject(i));
			}

			System.out.println(row);
		}

		watch.stop();
		System.out.println("Test Execute Big Data 700w Group by Time Elapsed: " + watch.getTime());
	}

	@Test
	public void testGroupByUseCsvJdbcFoo800w() throws SQLException {
		Properties props = new Properties();
		props.put("headerline", "a,b");
		props.put("suppressHeaders", "false");
		props.put("fileExtension", ".csv");
		props.put("commentChar", "#");
		props.put("columnTypes", "Integer,Integer");

		String path = CsvJdbcTest.class.getResource("/").toString();
		path = path.substring(5);

		String sql = "select avg(a) as \"avg_a\",b from foo500 group by b";

		StopWatch watch = new StopWatch();
		watch.start();

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + path, props);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		ResultSetMetaData rsmd = rs.getMetaData();
		while (rs.next()) {
			Map<String, Object> row = new HashMap<>();
			for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
				row.put(rsmd.getColumnName(i), rs.getObject(i));
			}

			System.out.println(row);
		}

		watch.stop();
		System.out.println("Test Execute Big Data 800w Group by Time Elapsed: " + watch.getTime());
	}

	@Test
	public void testGroupByUseCsvJdbcFoo1000w() throws SQLException {
		Properties props = new Properties();
		props.put("headerline", "a,b");
		props.put("suppressHeaders", "false");
		props.put("fileExtension", ".csv");
		props.put("commentChar", "#");
		props.put("columnTypes", "Integer,Integer");

		String path = CsvJdbcTest.class.getResource("/").toString();
		path = path.substring(5);

		String sql = "select avg(a) as \"avg_a\",b from foo1000 group by b";

		StopWatch watch = new StopWatch();
		watch.start();

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + path, props);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		ResultSetMetaData rsmd = rs.getMetaData();
		while (rs.next()) {
			Map<String, Object> row = new HashMap<>();
			for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
				row.put(rsmd.getColumnName(i), rs.getObject(i));
			}

			System.out.println(row);
		}

		watch.stop();
		System.out.println("Test Execute Big Data 1000w Group by Time Elapsed: " + watch.getTime());
	}

	@Test
	public void testGroupByUseCsvJdbcFoo2000w() throws SQLException {
		Properties props = new Properties();
		props.put("headerline", "a,b");
		props.put("suppressHeaders", "false");
		props.put("fileExtension", ".csv");
		props.put("commentChar", "#");
		props.put("columnTypes", "Integer,Integer");

		String path = CsvJdbcTest.class.getResource("/").toString();
		path = path.substring(5);

		String sql = "select avg(a) as \"avg_a\",b from foo2000 group by b";

		StopWatch watch = new StopWatch();
		watch.start();

		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + path, props);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		ResultSetMetaData rsmd = rs.getMetaData();
		while (rs.next()) {
			Map<String, Object> row = new HashMap<>();
			for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
				row.put(rsmd.getColumnName(i), rs.getObject(i));
			}

			System.out.println(row);
		}

		watch.stop();
		System.out.println("Test Execute Big Data 2000w Group by Time Elapsed: " + watch.getTime());
	}
}
