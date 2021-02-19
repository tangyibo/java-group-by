package com.github.tang.groupby.converter;

import com.github.tang.groupby.StringToNumberConverter;

/**
 * String到Integer类型的转换器
 * 
 * @author tang
 *
 */
public class StringToIntegerConverter implements StringToNumberConverter<Integer> {

	@Override
	public Integer convert(String s) throws NumberFormatException {
		return Integer.parseInt(s);
	}

}
