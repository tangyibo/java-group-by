package com.github.tang.groupby.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import com.github.tang.groupby.Aggregation;
import com.github.tang.groupby.DataRecordSet;
import com.github.tang.groupby.GroupByService;
import com.github.tang.groupby.StringToNumberConverter;

public class DefaultGroupByService extends AbstractGroupByService implements GroupByService {

	private Map<String, StringToNumberConverter<?>> groupByFieldsMap;

	private Map<String, Aggregation<?, ?>> queryFieldsMap;

	public DefaultGroupByService(DataRecordSet dataRecordSet) {
		super(dataRecordSet);
	}

	@Override
	public List<Map<String, String>> groupByInternal(Aggregation<?, ?>... aggregations) {
		if (null != groupByFieldsMap) {
			groupByFieldsMap.clear();
		}

		groupByFieldsMap = Arrays.asList(aggregations).stream().filter((a) -> a.getAggregationFunctionName() == null)
				.collect(Collectors.toMap(Aggregation::getAggregationFieldName, Aggregation::getConverter));

		queryFieldsMap = Arrays.asList(aggregations).stream()
				.collect(Collectors.toMap(a -> a.getAggregationFieldName(), a -> a));

		// 字段排序
		this.orderUseGroupByFields(aggregations);

		// System.out.println(dataRecordSet);

		// 归并聚合
		return this.aggregateData(aggregations);
	}

	/**
	 * 根据group by的字段升序排序
	 * 
	 * @param aggregations
	 */
	private void orderUseGroupByFields(Aggregation<?, ?>... aggregations) {
		Collections.sort(dataRecordSet.getContent(), new Comparator<String[]>() {

			@Override
			public int compare(String[] left, String[] rigth) {
				for (Map.Entry<String, StringToNumberConverter<?>> entry : groupByFieldsMap.entrySet()) {
					String field = entry.getKey();
					StringToNumberConverter<?> converter = entry.getValue();
					
					int index = dataRecordSet.getFieldIndex(field);
					
					Number o1 = converter.convert(left[index]);
					Number o2 = converter.convert(rigth[index]);

					// Here o1 and o2 is all implement Comparable
					@SuppressWarnings("rawtypes")
					Comparable c1 = (Comparable) o1;
					@SuppressWarnings("rawtypes")
					Comparable c2 = (Comparable) o2;

					@SuppressWarnings("unchecked")
					int ret = c1.compareTo(c2);
					if (0 != ret) {
						return ret;
					}
				}

				return 0;
			}

		});
	}

	/**
	 * 归并聚合计算
	 * 
	 * @param aggregations
	 * @return 计算结果集合
	 */
	private List<Map<String, String>> aggregateData(Aggregation<?, ?>... aggregations) {
		/* 存储计算结果的结果集 */
		List<Map<String, String>> result = new LinkedList<>();
		/* 获取group的所有字段 */
		Set<String> groupFields = groupByFieldsMap.keySet();
		/* 获取非group的所有字段 */
		Set<String> otherFields = queryFieldsMap.keySet().stream().filter((i) -> !groupFields.contains(i))
				.collect(Collectors.toSet());

		/* 记录聚合计算的起止数据游标 */
		int first = 0, last = 0;

		/**
		 * <p>
		 * 1、 遍历数据集
		 * </p>
		 * <p>
		 * 2、 归并字段
		 * </p>
		 * <p>
		 * 3、 聚合计算
		 * </p>
		 */
		int i;
		for (i = 0; i < dataRecordSet.getContent().size(); ++i) {
			String[] originRow = dataRecordSet.getContent().get(i);

			if (result.isEmpty()) {
				// 当首次结果集为空时，先将分组字段数据存储，整行记录不完整
				Map<String, String> targetRow = new HashMap<>();
				for (String gf : groupFields) {
					targetRow.put(gf, originRow[dataRecordSet.getFieldIndex(gf)]);
				}

				result.add(targetRow);
				continue;
			}

			if (canMergeToLast(originRow, result, groupFields)) {
				// 遇到可以聚合的行时，只需更新last游标即可
				last = i;
			} else {
				// 找到一个可以进行聚合计算的结果
				if (i > 0 && first <= last) {
					Map<String, String> targetRow = result.get(result.size() - 1);

					// 调用聚合函数进行计算,聚合函数由用户从外部传入
					for (String of : otherFields) {
						Aggregation<?, ?> aggregation = queryFieldsMap.get(of);
						List<String[]> subData = dataRecordSet.getSubContent(first, last + 1);
						Number v = aggregation.aggregation(dataRecordSet.getHeader(), subData);
						targetRow.put(aggregation.getAggregationFunctionName() + "_" + of, v.toString());
					}

					targetRow = new HashMap<>();
					for (String f : groupFields) {
						targetRow.put(f, originRow[dataRecordSet.getFieldIndex(f)]);
					}

					result.add(targetRow);
				}

				first = last + 1;
				last = first;
			}

		}

		/**
		 * 调用聚合函数补充最后一行缺失的字段值
		 */
		Map<String, String> targetRow = result.get(result.size() - 1);

		// 调用聚合函数进行计算
		for (String of : otherFields) {
			Aggregation<?, ?> aggregation = queryFieldsMap.get(of);
			List<String[]> subData = dataRecordSet.getSubContent(first, last + 1);
			Number v = aggregation.aggregation(dataRecordSet.getHeader(), subData);
			targetRow.put(aggregation.getAggregationFunctionName() + "_" + of, v.toString());
		}

		return result;
	}

	/**
	 * 判断是否可归并聚合
	 * 
	 * @param row
	 * @param result
	 * @return
	 */
	private boolean canMergeToLast(String[] row, List<Map<String, String>> result, Set<String> groupFields) {
		if (result.isEmpty()) {
			return false;
		}

		Map<String, String> lastRow = result.get(result.size() - 1);

		for (String f : groupFields) {
			String oldValue = lastRow.get(f);
			String newValue = row[dataRecordSet.getFieldIndex(f)];
			if (null == oldValue) {
				if (newValue != null) {
					return false;
				} else {
					return true;
				}
			}

			if (!oldValue.equals(newValue)) {
				return false;
			}
		}

		return true;
	}

}
