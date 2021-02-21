package com.github.tang.groupby.aggregation;

import java.util.List;
import java.util.Map;
import com.github.tang.groupby.StringToNumberConverter;

/**
 * int count(int[])聚合函数的实现
 * 
 * @author tang
 *
 */
public class IntegerCountIntegerAggregation extends AbstractAggregation<Integer, Integer> {

	public IntegerCountIntegerAggregation(String field, StringToNumberConverter<Integer> converter) {
		super(field, converter,"count");
	}

	@Override
	public Integer aggregation(Map<String, Integer> header, List<String[]> data) {
		return data.size();
	}

}
