package com.github.tang.groupby.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.csvreader.CsvReader;

/**
 * CSV文件操作工具类
 * 
 * @author tang
 *
 */
public final class CsvFileUtils {

	private CsvFileUtils() {
	}

	/**
	 * 读取指定文件路径的csv文件内容到数据结构List<Map<String, Integer>>中
	 * 
	 * @param filePath 文件路径名称
	 * @return 文件内容
	 * @throws IOException
	 * @throws NumberFormatException
	 */
	public static List<Map<String, String>> readCsvFile(String filePath, String... columns)
			throws NumberFormatException, IOException {
		List<Map<String, String>> content = new ArrayList<>(5000000);
		CsvReader csvReader = new CsvReader(filePath);
		if (csvReader.readHeaders()) {
			if (0 == columns.length) {
				columns = csvReader.getHeaders();
			}
		}

		while (csvReader.readRecord()) {
			Map<String, String> entry = new HashMap<>();
			for (String c : columns) {
				entry.put(c, csvReader.get(c));
			}

			content.add(entry);
		}

		return content;
	}

	// public static void writeCsvFile(String fileName, List<Map<String, Integer>> data) {
	//
	// }
}
