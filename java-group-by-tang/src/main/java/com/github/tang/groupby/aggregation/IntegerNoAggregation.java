package com.github.tang.groupby.aggregation;

import java.util.List;
import java.util.Map;
import com.github.tang.groupby.StringToNumberConverter;

/**
 * 不做聚合函数的实现
 * 
 * @author tang
 *
 */
public class IntegerNoAggregation extends AbstractAggregation<Integer, Integer> {

	public IntegerNoAggregation(String field, StringToNumberConverter<Integer> converter) {
		super(field, converter);
	}

	@Override
	public Integer aggregation(Map<String, Integer> header, List<String[]> data) {
		return converter.convert(data.get(0)[header.get(fieldName)]);
	}

}
