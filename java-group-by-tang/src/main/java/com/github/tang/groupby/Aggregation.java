package com.github.tang.groupby;

import java.util.List;
import java.util.Map;

/**
 * 聚合函数接口
 * 
 * <h3>Usage Examples</h3>
 * 
 * <pre>
 *  {@code
 * public class IntegerSumAggregationFunction extends AbstractAggregation<Integer, Integer> {
 * 
 *	public IntegerSumAggregationFunction(String field,StringToNumberConverter<Integer> converter) {
 *		super(field,converter,"sum");
 *	}
 * 
 *	&#64;Override
 *	public Integer aggregation(List<Map<String, String>> data) {
 *		return data.stream().mapToInt((item) -> converter.convert(item.get(fieldName))).sum();
 *	}
 * }
 * </pre>
 * 
 * @author tang
 *
 * @param <R> 计算结果的数据类型
 * @param <T> String转换后的输入数据类型
 */
public interface Aggregation<R extends Number, T extends Number> {

	/**
	 * 聚合函数
	 * 
	 * @param data 数据
	 * @return 聚合计算的结果
	 */
	R aggregation(List<Map<String, String>> data);

	/**
	 * 聚合的字段名
	 * 
	 * @return
	 */
	String getAggregationFieldName();

	/**
	 * 获取类型转换器
	 * 
	 * @return
	 */
	StringToNumberConverter<T> getConverter();

	/**
	 * 聚合函数的名称
	 * 
	 * @return
	 */
	String getAggregationFunctionName();
}
