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
public class CountAggregation<T extends Number> extends AbstractAggregation<Integer, T> {

	public CountAggregation(String field, StringToNumberConverter<T> converter) {
		super(field, converter, "count");
	}

	@Override
	public Integer aggregation(Map<String, Integer> header, List<String[]> data) {
		return data.size();
	}

}
