package com.github.tang.groupby.aggregation;

import java.util.List;
import java.util.Map;
import com.github.tang.groupby.StringToNumberConverter;

/**
 * double avg(int[])聚合函数的实现
 * 
 * @author tang
 *
 */
public class DoubleAvgIntegerAggregation extends AbstractAggregation<Double, Integer> {

	public DoubleAvgIntegerAggregation(String field, StringToNumberConverter<Integer> converter) {
		super(field, converter, "avg");
	}

	@Override
	public Double aggregation(List<Map<String, String>> data) {
		Integer sum = 0;
		for (Map<String, String> row : data) {
			Integer value = converter.convert(row.get(fieldName));
			sum += value.intValue();
		}

		return sum.doubleValue() / data.size();
	}

}
