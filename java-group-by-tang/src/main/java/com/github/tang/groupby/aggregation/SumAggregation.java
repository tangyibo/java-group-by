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
public class SumAggregation<T extends Number> extends AbstractAggregation<T, T> {

	public SumAggregation(String field, StringToNumberConverter<T> converter) {
		super(field, converter,"sum");
	}

	@Override
	public T aggregation(Map<String, Integer> header, List<String[]> data) {
		T initValue = converter.convert(data.get(0)[header.get(fieldName)]);
		Double sum = initValue.doubleValue();
		for (int i = 1; i < data.size(); ++i) {
			T value = converter.convert(data.get(i)[header.get(fieldName)]);
			sum += value.doubleValue();
		}

		return (T) sum;
	}

}
