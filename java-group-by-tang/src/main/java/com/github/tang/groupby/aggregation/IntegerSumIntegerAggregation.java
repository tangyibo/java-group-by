package com.github.tang.groupby.aggregation;

import java.util.List;
import java.util.Map;

import com.github.tang.groupby.StringToNumberConverter;

/**
 * int sum(int[])函数数的实现
 * 
 * @author tang
 *
 */
public class IntegerSumIntegerAggregation extends AbstractAggregation<Integer, Integer> {

	public IntegerSumIntegerAggregation(String field, StringToNumberConverter<Integer> converter) {
		super(field, converter,"sum");
	}

	@Override
	public Integer aggregation(List<Map<String, String>> data) {
		return data.stream().mapToInt((item) -> converter.convert(item.get(fieldName))).sum();
	}

}
