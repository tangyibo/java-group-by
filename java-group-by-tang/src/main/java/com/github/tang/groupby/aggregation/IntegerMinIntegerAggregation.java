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
public class IntegerMinIntegerAggregation extends AbstractAggregation<Integer, Integer> {

	public IntegerMinIntegerAggregation(String field, StringToNumberConverter<Integer> converter) {
		super(field, converter, "min");
	}

	@Override
	public Integer aggregation(Map<String, Integer> header, List<String[]> data) {
		Integer min = Integer.MAX_VALUE;
		for (String[] row : data) {
			Integer value = converter.convert(row[header.get(fieldName)]);
			if (value.intValue() < min.intValue()) {
				min = value;
			}
			;
		}

		return min;
	}

}
