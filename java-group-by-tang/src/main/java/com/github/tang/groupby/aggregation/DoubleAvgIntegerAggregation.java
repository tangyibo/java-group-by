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
	public Double aggregation(Map<String, Integer> header, List<String[]> data) {
		Integer sum = 0;
		for (String[] row : data) {
			Integer value = converter.convert(row[header.get(fieldName)]);
			sum += value.intValue();
		}

		return sum.doubleValue() / data.size();
	}

}
