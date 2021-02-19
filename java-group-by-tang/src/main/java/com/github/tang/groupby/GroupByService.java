package com.github.tang.groupby;

import java.util.List;
import java.util.Map;

/**
 * Group by
 * 
 * @author tang
 *
 */
public interface GroupByService {

	/**
	 * group by 函数
	 * 
	 * @param aggregation 聚合函数
	 * @return
	 */
	List<Map<String, String>> groupBy(Aggregation<?, ?>... aggregations);
}
