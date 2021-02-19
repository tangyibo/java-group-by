package com.github.tang.groupby;

/**
 * String到Number数字类型转换器
 * 
 * @author tang
 *
 * @param <T> 要转换的具体Number类型
 */
public interface StringToNumberConverter<T extends Number> {

	/**
	 * 转换函数
	 * 
	 * @param s String类型的文本串
	 * @return
	 * @throws NumberFormatException
	 */
	T convert(String s) throws NumberFormatException;

}
