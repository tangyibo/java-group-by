package com.github.tang.groupby.converter;

import com.github.tang.groupby.StringToNumberConverter;

/**
 * String到Short类型的转换器
 * 
 * @author tang
 *
 */
public class StringToShortConverter implements StringToNumberConverter<Short> {

	@Override
	public Short convert(String s) throws NumberFormatException {
		return Short.parseShort(s);
	}

}
