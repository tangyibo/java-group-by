package com.github.tang.calcite.groupby;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Before;
import org.junit.Test;

public class CalciteTest {

	@Before
    public void init(){
		System.out.println("===========================Calcite=========================================");
    }
	
	@Test
	public void testGroupByUseCalciteCsvFile() throws SQLException {
		Properties config = new Properties();
        config.put("model", CalciteTest.class.getClassLoader().getResource("config.json").getPath());
        config.put("caseSensitive", "false");

        StopWatch watch = new StopWatch();
		watch.start();
		
		Connection connection = DriverManager.getConnection("jdbc:calcite:", config);
		CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

		String sql = "select avg(a) as \"avg_a\",b from csv.test group by b";
		Statement statement = calciteConnection.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		ResultSetMetaData rsmd=rs.getMetaData();
		
		while(rs.next()) {
			Map<String,Object> row=new HashMap<>();
			for(int i=1;i<=rsmd.getColumnCount();++i) {
				row.put(rsmd.getColumnName(i), rs.getObject(i));
			}
			
			System.out.println(row);
		}
		
		rs.close();
		statement.close();
		connection.close();
		
		watch.stop();
		System.out.println("Test Execute Group by Time Elapsed: " + watch.getTime());
	}


	@Test
	public void testGroupByUseCalciteCsvFile100w() throws SQLException {
		Properties config = new Properties();
        config.put("model", CalciteTest.class.getClassLoader().getResource("config.json").getPath());
        config.put("caseSensitive", "false");

        StopWatch watch = new StopWatch();
		watch.start();
		
		Connection connection = DriverManager.getConnection("jdbc:calcite:", config);
		CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

		String sql = "select avg(a) as \"avg_a\",b from csv.\"foo100\" group by b";
		Statement statement = calciteConnection.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		ResultSetMetaData rsmd=rs.getMetaData();
		
		while(rs.next()) {
			Map<String,Object> row=new HashMap<>();
			for(int i=1;i<=rsmd.getColumnCount();++i) {
				row.put(rsmd.getColumnName(i), rs.getObject(i));
			}
			
			System.out.println(row);
		}
		
		rs.close();
		statement.close();
		connection.close();
		
		watch.stop();
		System.out.println("Test Execute Big Data 100w Group by Time Elapsed: " + watch.getTime());
	}
	

	@Test
	public void testGroupByUseCalciteCsvFile500w() throws SQLException {
		Properties config = new Properties();
        config.put("model", CalciteTest.class.getClassLoader().getResource("config.json").getPath());
        config.put("caseSensitive", "false");

        StopWatch watch = new StopWatch();
		watch.start();
		
		Connection connection = DriverManager.getConnection("jdbc:calcite:", config);
		CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

		String sql = "select avg(a) as \"avg_a\",b from csv.\"foo500\" group by b";
		Statement statement = calciteConnection.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		ResultSetMetaData rsmd=rs.getMetaData();
		
		while(rs.next()) {
			Map<String,Object> row=new HashMap<>();
			for(int i=1;i<=rsmd.getColumnCount();++i) {
				row.put(rsmd.getColumnName(i), rs.getObject(i));
			}
			
			System.out.println(row);
		}
		
		rs.close();
		statement.close();
		connection.close();
		
		watch.stop();
		System.out.println("Test Execute Big Data 500w Group by Time Elapsed: " + watch.getTime());
	}
	
	@Test
	public void testGroupByUseCalciteCsvFile600w() throws SQLException {
		Properties config = new Properties();
        config.put("model", CalciteTest.class.getClassLoader().getResource("config.json").getPath());
        config.put("caseSensitive", "false");

        StopWatch watch = new StopWatch();
		watch.start();
		
		Connection connection = DriverManager.getConnection("jdbc:calcite:", config);
		CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

		String sql = "select avg(a) as \"avg_a\",b from csv.\"foo600\" group by b";
		Statement statement = calciteConnection.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		ResultSetMetaData rsmd=rs.getMetaData();
		
		while(rs.next()) {
			Map<String,Object> row=new HashMap<>();
			for(int i=1;i<=rsmd.getColumnCount();++i) {
				row.put(rsmd.getColumnName(i), rs.getObject(i));
			}
			
			System.out.println(row);
		}
		
		rs.close();
		statement.close();
		connection.close();
		
		watch.stop();
		System.out.println("Test Execute Big Data 600w Group by Time Elapsed: " + watch.getTime());
	}
	
	@Test
	public void testGroupByUseCalciteCsvFile700w() throws SQLException {
		Properties config = new Properties();
        config.put("model", CalciteTest.class.getClassLoader().getResource("config.json").getPath());
        config.put("caseSensitive", "false");

        StopWatch watch = new StopWatch();
		watch.start();
		
		Connection connection = DriverManager.getConnection("jdbc:calcite:", config);
		CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

		String sql = "select avg(a) as \"avg_a\",b from csv.\"foo700\" group by b";
		Statement statement = calciteConnection.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		ResultSetMetaData rsmd=rs.getMetaData();
		
		while(rs.next()) {
			Map<String,Object> row=new HashMap<>();
			for(int i=1;i<=rsmd.getColumnCount();++i) {
				row.put(rsmd.getColumnName(i), rs.getObject(i));
			}
			
			System.out.println(row);
		}
		
		rs.close();
		statement.close();
		connection.close();
		
		watch.stop();
		System.out.println("Test Execute Big Data 700w Group by Time Elapsed: " + watch.getTime());
	}
	
	@Test
	public void testGroupByUseCalciteCsvFile800w() throws SQLException {
		Properties config = new Properties();
        config.put("model", CalciteTest.class.getClassLoader().getResource("config.json").getPath());
        config.put("caseSensitive", "false");

        StopWatch watch = new StopWatch();
		watch.start();
		
		Connection connection = DriverManager.getConnection("jdbc:calcite:", config);
		CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

		String sql = "select avg(a) as \"avg_a\",b from csv.\"foo800\" group by b";
		Statement statement = calciteConnection.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		ResultSetMetaData rsmd=rs.getMetaData();
		
		while(rs.next()) {
			Map<String,Object> row=new HashMap<>();
			for(int i=1;i<=rsmd.getColumnCount();++i) {
				row.put(rsmd.getColumnName(i), rs.getObject(i));
			}
			
			System.out.println(row);
		}
		
		rs.close();
		statement.close();
		connection.close();
		
		watch.stop();
		System.out.println("Test Execute Big Data 800w Group by Time Elapsed: " + watch.getTime());
	}
	
	
	@Test
	public void testGroupByUseCalciteCsvFile1000w() throws SQLException {
		Properties config = new Properties();
        config.put("model", CalciteTest.class.getClassLoader().getResource("config.json").getPath());
        config.put("caseSensitive", "false");

        StopWatch watch = new StopWatch();
		watch.start();
		
		Connection connection = DriverManager.getConnection("jdbc:calcite:", config);
		CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

		String sql = "select avg(a) as \"avg_a\",b from csv.\"foo1000\" group by b";
		Statement statement = calciteConnection.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		ResultSetMetaData rsmd=rs.getMetaData();
		
		while(rs.next()) {
			Map<String,Object> row=new HashMap<>();
			for(int i=1;i<=rsmd.getColumnCount();++i) {
				row.put(rsmd.getColumnName(i), rs.getObject(i));
			}
			
			System.out.println(row);
		}
		
		rs.close();
		statement.close();
		connection.close();
		
		watch.stop();
		System.out.println("Test Execute Big Data 1000w Group by Time Elapsed: " + watch.getTime());
	}
	
	
	@Test
	public void testGroupByUseCalciteCsvFile2000w() throws SQLException {
		Properties config = new Properties();
        config.put("model", CalciteTest.class.getClassLoader().getResource("config.json").getPath());
        config.put("caseSensitive", "false");

        StopWatch watch = new StopWatch();
		watch.start();
		
		Connection connection = DriverManager.getConnection("jdbc:calcite:", config);
		CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

		String sql = "select avg(a) as \"avg_a\",b from csv.\"foo2000\" group by b";
		Statement statement = calciteConnection.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		ResultSetMetaData rsmd=rs.getMetaData();
		
		while(rs.next()) {
			Map<String,Object> row=new HashMap<>();
			for(int i=1;i<=rsmd.getColumnCount();++i) {
				row.put(rsmd.getColumnName(i), rs.getObject(i));
			}
			
			System.out.println(row);
		}
		
		rs.close();
		statement.close();
		connection.close();
		
		watch.stop();
		System.out.println("Test Execute Big Data 2000w Group by Time Elapsed: " + watch.getTime());
	}
}
