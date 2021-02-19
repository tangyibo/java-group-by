package com.github.tang.groupby.aggregation;

import com.github.tang.groupby.Aggregation;
import com.github.tang.groupby.StringToNumberConverter;

public abstract class AbstractAggregation<R extends Number, T extends Number> implements Aggregation<R, T> {

	protected String fieldName;
	protected StringToNumberConverter<T> converter;
	protected String functionName;

	public AbstractAggregation(String field, StringToNumberConverter<T> converter) {
		this(field, converter, null);
	}

	public AbstractAggregation(String field, StringToNumberConverter<T> converter, String functionName) {
		this.fieldName = field;
		this.converter = converter;
		this.functionName = functionName;
	}

	@Override
	public String getAggregationFieldName() {
		return this.fieldName;
	}

	@Override
	public String getAggregationFunctionName() {
		return this.functionName;
	}

	@Override
	public StringToNumberConverter<T> getConverter() {
		return this.converter;
	}
}
