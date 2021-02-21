package com.github.tang.groupby.aggregation;

import java.util.List;
import java.util.Map;
import com.github.tang.groupby.StringToNumberConverter;

/**
 * int max(int[])聚合函数的实现
 * 
 * @author tang
 *
 */
public class MaxAggregation<T extends Number> extends AbstractAggregation<T, T> {

	public MaxAggregation(String field, StringToNumberConverter<T> converter) {
		super(field, converter, "max");
	}

	@Override
	public T aggregation(Map<String, Integer> header, List<String[]> data) {
		T max = converter.convert(data.get(0)[header.get(fieldName)]);
		for (String[] row : data) {
			T value = converter.convert(row[header.get(fieldName)]);
			if (value.doubleValue() > max.doubleValue()) {
				max = value;
			}
		}

		return max;
	}

}
