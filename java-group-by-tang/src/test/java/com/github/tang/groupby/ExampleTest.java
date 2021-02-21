package com.github.tang.groupby;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Before;
import org.junit.Test;
import com.github.tang.groupby.aggregation.DoubleAvgIntegerAggregation;
import com.github.tang.groupby.aggregation.IntegerCountIntegerAggregation;
import com.github.tang.groupby.aggregation.IntegerMaxIntegerAggregation;
import com.github.tang.groupby.aggregation.IntegerMinIntegerAggregation;
import com.github.tang.groupby.aggregation.IntegerNoAggregation;
import com.github.tang.groupby.aggregation.IntegerSumIntegerAggregation;
import com.github.tang.groupby.converter.StringToIntegerConverter;
import com.github.tang.groupby.core.DefaultGroupByService;
import com.github.tang.groupby.util.CsvFileUtils;

/**
 * 功能性单元测试类
 * 
 * @author tang
 *
 */
public class ExampleTest {

	@Before
    public void init(){
		System.out.println("=================================Example=====================================");
    }
	
	@Test
	public void testGroupByServiceWithTestAvg() throws NumberFormatException, IOException {
		StopWatch watch = new StopWatch();
		
		watch.start();
		URL resource = ExampleTest.class.getClassLoader().getResource("test.csv");
		DataRecordSet data=CsvFileUtils.readCsvFile(resource.getFile());

		GroupByService service = new DefaultGroupByService(data);
		/* SELECT avg(a) as `avg_a`,b FROM `test` GROUP BY b; */
		List<Map<String, String>> r = service.groupBy(new DoubleAvgIntegerAggregation("a",new StringToIntegerConverter()), new IntegerNoAggregation("b",new StringToIntegerConverter()));
		System.out.println("Result table: ");
		r.stream().forEach((i)->System.out.println(i));
		watch.stop();
		System.out.println("Test Execute Avg() Group by Time Elapsed: " + watch.getTime());
	}
	
	@Test
	public void testGroupByServiceWithTestMin() throws NumberFormatException, IOException {
		StopWatch watch = new StopWatch();
		
		watch.start();
		URL resource = ExampleTest.class.getClassLoader().getResource("test.csv");
		DataRecordSet data=CsvFileUtils.readCsvFile(resource.getFile());

		GroupByService service = new DefaultGroupByService(data);
		/* SELECT min(a) as `avg_a`,b FROM `test` GROUP BY b; */
		List<Map<String, String>> r = service.groupBy(new IntegerMinIntegerAggregation("a",new StringToIntegerConverter()), new IntegerNoAggregation("b",new StringToIntegerConverter()));
		System.out.println("Result table: ");
		r.stream().forEach((i)->System.out.println(i));
		watch.stop();
		System.out.println("Test Execute Min() Group by Time Elapsed: " + watch.getTime());
	}
	
	@Test
	public void testGroupByServiceWithTestMax() throws NumberFormatException, IOException {
		StopWatch watch = new StopWatch();
		
		watch.start();
		URL resource = ExampleTest.class.getClassLoader().getResource("test.csv");
		DataRecordSet data=CsvFileUtils.readCsvFile(resource.getFile());

		GroupByService service = new DefaultGroupByService(data);
		/* SELECT max(a) as `avg_a`,b FROM `test` GROUP BY b; */
		List<Map<String, String>> r = service.groupBy(new IntegerMaxIntegerAggregation("a",new StringToIntegerConverter()), new IntegerNoAggregation("b",new StringToIntegerConverter()));
		System.out.println("Result table: ");
		r.stream().forEach((i)->System.out.println(i));
		watch.stop();
		System.out.println("Test Execute Max() Group by Time Elapsed: " + watch.getTime());
	}
	
	@Test
	public void testGroupByServiceWithTestSum() throws NumberFormatException, IOException {
		StopWatch watch = new StopWatch();
		
		watch.start();
		URL resource = ExampleTest.class.getClassLoader().getResource("test.csv");
		DataRecordSet data=CsvFileUtils.readCsvFile(resource.getFile());

		GroupByService service = new DefaultGroupByService(data);
		/* SELECT sum(a) as `avg_a`,b FROM `test` GROUP BY b; */
		List<Map<String, String>> r = service.groupBy(new IntegerSumIntegerAggregation("a",new StringToIntegerConverter()), new IntegerNoAggregation("b",new StringToIntegerConverter()));
		System.out.println("Result table: ");
		r.stream().forEach((i)->System.out.println(i));
		watch.stop();
		System.out.println("Test Execute sum() Group by Time Elapsed: " + watch.getTime());
	}
	
	@Test
	public void testGroupByServiceWithTestCount() throws NumberFormatException, IOException {
		StopWatch watch = new StopWatch();
		
		watch.start();
		URL resource = ExampleTest.class.getClassLoader().getResource("test.csv");
		DataRecordSet data=CsvFileUtils.readCsvFile(resource.getFile());

		GroupByService service = new DefaultGroupByService(data);
		/* SELECT count(a) as `avg_a`,b FROM `test` GROUP BY b; */
		List<Map<String, String>> r = service.groupBy(new IntegerCountIntegerAggregation("a",new StringToIntegerConverter()), new IntegerNoAggregation("b",new StringToIntegerConverter()));
		System.out.println("Result table: ");
		r.stream().forEach((i)->System.out.println(i));
		watch.stop();
		System.out.println("Test Execute count() Group by Time Elapsed: " + watch.getTime());
	}
	
}
