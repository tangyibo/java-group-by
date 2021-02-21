package com.github.tang.groupby.converter;

import com.github.tang.groupby.StringToNumberConverter;

/**
 * String到Long类型的转换器
 * 
 * @author tang
 *
 */
public class StringToLongConverter implements StringToNumberConverter<Long> {

	@Override
	public Long convert(String s) throws NumberFormatException {
		return Long.parseLong(s);
	}

}
