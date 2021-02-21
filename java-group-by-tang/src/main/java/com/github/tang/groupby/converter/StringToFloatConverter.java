package com.github.tang.groupby.converter;

import com.github.tang.groupby.StringToNumberConverter;

/**
 * String到Float类型的转换器
 * 
 * @author tang
 *
 */
public class StringToFloatConverter implements StringToNumberConverter<Float> {

	@Override
	public Float convert(String s) throws NumberFormatException {
		return Float.parseFloat(s);
	}

}
