package com.github.tang.groupby.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.csvreader.CsvReader;
import com.github.tang.groupby.DataRecordSet;

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
	 * 读取指定文件路径的csv文件内容到数据结构DataRecordSet中
	 * 
	 * @param filePath 文件路径名称
	 * @return 文件内容
	 * @throws IOException
	 */
	public static DataRecordSet readCsvFile(String filePath, String... columns) throws IOException {
		CsvReader csvReader = new CsvReader(filePath);
		if (csvReader.readHeaders()) {
			if (0 == columns.length) {
				columns = csvReader.getHeaders();
			}
		}

		Map<String, Integer> header = new HashMap<>();
		for (int i = 0; i < columns.length; ++i) {
			header.put(columns[i], i);
		}

		List<String[]> content = new ArrayList<>();
		try {
			while (csvReader.readRecord()) {
				String[] row = new String[columns.length];
				for (int i = 0; i < columns.length; ++i) {
					row[i] = csvReader.get(columns[i]);
				}

				content.add(row);
			}
		} catch (OutOfMemoryError e) {
			System.err.println("OOM happened when load data count=" + content.size());
			throw e;
		}

		return new DataRecordSet(header, content);
	}

	// public static void writeCsvFile(String fileName, DataRecordSet records) {
	//
	// }
}
