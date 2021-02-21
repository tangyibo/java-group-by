package com.github.tang.groupby.aggregation;

import java.util.List;
import java.util.Map;
import com.github.tang.groupby.StringToNumberConverter;

/**
 * int min(int[])聚合函数的实现
 * 
 * @author tang
 *
 */
public class MinAggregation<T extends Number> extends AbstractAggregation<T, T> {

	public MinAggregation(String field, StringToNumberConverter<T> converter) {
		super(field, converter, "min");
	}

	@Override
	public T aggregation(Map<String, Integer> header, List<String[]> data) {
		T min = converter.convert(data.get(0)[header.get(fieldName)]);
		for (String[] row : data) {
			T value = converter.convert(row[header.get(fieldName)]);
			if (value.intValue() < min.intValue()) {
				min = value;
			}
		}

		return min;
	}

}
