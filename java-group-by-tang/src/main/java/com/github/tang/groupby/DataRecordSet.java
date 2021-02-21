package com.github.tang.groupby;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 数据集在内存中的存储结构
 * 
 * @author tang
 *
 */
public class DataRecordSet {

	/* 数据集中的头部元数据，key:字段名,value:位置索引号 */
	private Map<String, Integer> header;

	/* 数据集中的数据部分 */
	private List<String[]> content;

	public DataRecordSet() {
		this.header = new HashMap<>();
		this.content = new ArrayList<>();
	}

	public DataRecordSet(Map<String, Integer> header) {
		this.header = new HashMap<>(header);
		this.content = new ArrayList<>();
	}

	public DataRecordSet(Map<String, Integer> header, List<String[]> content) {
		this.header = new HashMap<>(header);

		// 先简单这么处理
		this.content = Objects.requireNonNull(content,"Invalid content");
	}

	public Map<String, Integer> getHeader() {
		return this.header;
	}
	
	public int getFieldIndex(String field) {
		return this.header.get(field);
	}

	public List<String[]> getContent() {
		return this.content;
	}

	public List<String[]> getSubContent(int first, int last) {
		if (first < 0 && last >= this.content.size()) {
			throw new IllegalArgumentException("Invalid cursor postion!");
		}

		return this.content.subList(first, last);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Object[] row : this.content) {
			int i=0;
			sb.append("{");
			for (Map.Entry<String, Integer> entry : this.header.entrySet()) {
				if (i > 0) {
					sb.append(", ");
				}
				
				String field = entry.getKey();
				Object value = row[entry.getValue()];
				sb.append(String.format("%s = %s", field, value));

				i++;
			}

			sb.append("}\n");
		}

		return sb.toString();
	}
}
