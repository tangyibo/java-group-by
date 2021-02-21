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
public class NoAggregation<T extends Number> extends AbstractAggregation<T, T> {

	public NoAggregation(String field, StringToNumberConverter<T> converter) {
		super(field, converter);
	}

	@Override
	public T aggregation(Map<String, Integer> header, List<String[]> data) {
		return converter.convert(data.get(0)[header.get(fieldName)]);
	}

}
