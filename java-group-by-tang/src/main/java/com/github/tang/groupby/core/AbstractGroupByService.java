package com.github.tang.groupby.core;

import java.util.List;
import java.util.Map;

import com.github.tang.groupby.Aggregation;
import com.github.tang.groupby.DataRecordSet;
import com.github.tang.groupby.GroupByService;

public abstract class AbstractGroupByService implements GroupByService {

	/* 存储csv文件中加载到内存的数据对象 */
	protected DataRecordSet dataRecordSet;

	public AbstractGroupByService(DataRecordSet dataRecordSet) {
		this.dataRecordSet = dataRecordSet;
	}

	@Override
	public List<Map<String, String>> groupBy(Aggregation<?, ?>... aggregations) {
		if (aggregations.length <= 0) {
			throw new IllegalArgumentException("查询的列名不能为空");
		}

		return this.groupByInternal(aggregations);
	}

	public abstract List<Map<String, String>> groupByInternal(Aggregation<?, ?>... aggregations);
}
