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
public class IntegerMaxIntegerAggregation extends AbstractAggregation<Integer, Integer> {

	public IntegerMaxIntegerAggregation(String field, StringToNumberConverter<Integer> converter) {
		super(field, converter,"max");
	}

	@Override
	public Integer aggregation(List<Map<String, String>> data) {
		Integer max = Integer.MIN_VALUE;
		for (Map<String, String> row : data) {
			Integer value = converter.convert(row.get(fieldName));
			if(value.intValue()>max.intValue()) {
				max=value;
			};
		}
		
		return max;
	}

}
