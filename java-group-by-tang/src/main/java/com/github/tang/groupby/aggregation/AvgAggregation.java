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
public class AvgAggregation<T extends Number> extends AbstractAggregation<Double, T> {

	public AvgAggregation(String field, StringToNumberConverter<T> converter) {
		super(field, converter, "avg");
	}

	@Override
	public Double aggregation(Map<String, Integer> header, List<String[]> data) {
		Double sum = new Double(0);
		for (String[] row : data) {
			T value = converter.convert(row[header.get(fieldName)]);
			sum += value.doubleValue();
		}

		return sum.doubleValue() / data.size();
	}

}
