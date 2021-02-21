package com.github.tang.groupby;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import com.github.tang.groupby.aggregation.DoubleAvgIntegerAggregation;
import com.github.tang.groupby.aggregation.IntegerNoAggregation;
import com.github.tang.groupby.converter.StringToIntegerConverter;
import com.github.tang.groupby.core.DefaultGroupByService;
import com.github.tang.groupby.util.CsvFileUtils;

/**
 * 大数据量性能测试类
 * 
 * @author tang
 *
 */
@FixMethodOrder(MethodSorters.DEFAULT)
public class PerformanceTest {

	@Before
    public void init(){
		System.out.println("=================================Performance=====================================");
    }
	
	@Test
	public void testGroupByServiceWithFooAvg100w() throws NumberFormatException, IOException {
		StopWatch watch = new StopWatch();
		
		watch.start();
		URL resource = ExampleTest.class.getClassLoader().getResource("foo100.csv");
		DataRecordSet data=CsvFileUtils.readCsvFile(resource.getFile());

		GroupByService service = new DefaultGroupByService(data);
		/* SELECT avg(a) as `avg_a`,b FROM `foo` GROUP BY b; */
		List<Map<String, String>> r = service.groupBy(new DoubleAvgIntegerAggregation("a",new StringToIntegerConverter()), new IntegerNoAggregation("b",new StringToIntegerConverter()));
		System.out.println("Result table: ");
		r.stream().forEach((i)->System.out.println(i));
		watch.stop();
		System.out.println("Test Execute Big Data 100w Avg() Group by Time Elapsed(ms) : " + watch.getTime());
	}
	
	@Test
	public void testGroupByServiceWithFooAvg500w() throws NumberFormatException, IOException {
		StopWatch watch = new StopWatch();
		
		watch.start();
		URL resource = ExampleTest.class.getClassLoader().getResource("foo500.csv");
		DataRecordSet data=CsvFileUtils.readCsvFile(resource.getFile());

		GroupByService service = new DefaultGroupByService(data);
		/* SELECT avg(a) as `avg_a`,b FROM `foo` GROUP BY b; */
		List<Map<String, String>> r = service.groupBy(new DoubleAvgIntegerAggregation("a",new StringToIntegerConverter()), new IntegerNoAggregation("b",new StringToIntegerConverter()));
		System.out.println("Result table: ");
		r.stream().forEach((i)->System.out.println(i));
		watch.stop();
		System.out.println("Test Execute Big Data 500w Avg() Group by Time Elapsed(ms) : " + watch.getTime());
	}
	
	@Test
	public void testGroupByServiceWithFooAvg600w() throws NumberFormatException, IOException {
		StopWatch watch = new StopWatch();
		
		watch.start();
		URL resource = ExampleTest.class.getClassLoader().getResource("foo600.csv");
		DataRecordSet data=CsvFileUtils.readCsvFile(resource.getFile());

		GroupByService service = new DefaultGroupByService(data);
		/* SELECT avg(a) as `avg_a`,b FROM `foo` GROUP BY b; */
		List<Map<String, String>> r = service.groupBy(new DoubleAvgIntegerAggregation("a",new StringToIntegerConverter()), new IntegerNoAggregation("b",new StringToIntegerConverter()));
		System.out.println("Result table: ");
		r.stream().forEach((i)->System.out.println(i));
		watch.stop();
		System.out.println("Test Execute Big Data 600w Avg() Group by Time Elapsed(ms) : " + watch.getTime());
	}
	
	@Test
	public void testGroupByServiceWithFooAvg700w() throws NumberFormatException, IOException {
		StopWatch watch = new StopWatch();
		
		watch.start();
		URL resource = ExampleTest.class.getClassLoader().getResource("foo700.csv");
		DataRecordSet data=CsvFileUtils.readCsvFile(resource.getFile());

		GroupByService service = new DefaultGroupByService(data);
		/* SELECT avg(a) as `avg_a`,b FROM `foo` GROUP BY b; */
		List<Map<String, String>> r = service.groupBy(new DoubleAvgIntegerAggregation("a",new StringToIntegerConverter()), new IntegerNoAggregation("b",new StringToIntegerConverter()));
		System.out.println("Result table: ");
		r.stream().forEach((i)->System.out.println(i));
		watch.stop();
		System.out.println("Test Execute Big Data 700w Avg() Group by Time Elapsed(ms) : " + watch.getTime());
	}
	
	@Test
	public void testGroupByServiceWithFooAvg800w() throws NumberFormatException, IOException {
		StopWatch watch = new StopWatch();
		
		watch.start();
		URL resource = ExampleTest.class.getClassLoader().getResource("foo800.csv");
		DataRecordSet data=CsvFileUtils.readCsvFile(resource.getFile());

		GroupByService service = new DefaultGroupByService(data);
		/* SELECT avg(a) as `avg_a`,b FROM `foo` GROUP BY b; */
		List<Map<String, String>> r = service.groupBy(new DoubleAvgIntegerAggregation("a",new StringToIntegerConverter()), new IntegerNoAggregation("b",new StringToIntegerConverter()));
		System.out.println("Result table: ");
		r.stream().forEach((i)->System.out.println(i));
		watch.stop();
		System.out.println("Test Execute Big Data 800w Avg() Group by Time Elapsed(ms) : " + watch.getTime());
	}
	
	@Test
	public void testGroupByServiceWithFooAvg1000w() throws NumberFormatException, IOException {
		StopWatch watch = new StopWatch();
		
		watch.start();
		URL resource = ExampleTest.class.getClassLoader().getResource("foo1000.csv");
		DataRecordSet data=CsvFileUtils.readCsvFile(resource.getFile());

		GroupByService service = new DefaultGroupByService(data);
		/* SELECT avg(a) as `avg_a`,b FROM `foo` GROUP BY b; */
		List<Map<String, String>> r = service.groupBy(new DoubleAvgIntegerAggregation("a",new StringToIntegerConverter()), new IntegerNoAggregation("b",new StringToIntegerConverter()));
		System.out.println("Result table: ");
		r.stream().forEach((i)->System.out.println(i));
		watch.stop();
		System.out.println("Test Execute Big Data 1000w Avg() Group by Time Elapsed(ms) : " + watch.getTime());
	}
	
	@Test
	public void testGroupByServiceWithFooAvg2000w() throws NumberFormatException, IOException {
		StopWatch watch = new StopWatch();
		
		watch.start();
		URL resource = ExampleTest.class.getClassLoader().getResource("foo2000.csv");
		DataRecordSet data=CsvFileUtils.readCsvFile(resource.getFile());

		GroupByService service = new DefaultGroupByService(data);
		/* SELECT avg(a) as `avg_a`,b FROM `foo` GROUP BY b; */
		List<Map<String, String>> r = service.groupBy(new DoubleAvgIntegerAggregation("a",new StringToIntegerConverter()), new IntegerNoAggregation("b",new StringToIntegerConverter()));
		System.out.println("Result table: ");
		r.stream().forEach((i)->System.out.println(i));
		watch.stop();
		System.out.println("Test Execute Big Data 2000w Avg() Group by Time Elapsed(ms) : " + watch.getTime());
	}
}
