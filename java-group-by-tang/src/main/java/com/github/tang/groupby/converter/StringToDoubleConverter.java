package com.github.tang.groupby.converter;

import com.github.tang.groupby.StringToNumberConverter;

/**
 * String到Double类型的转换器
 * 
 * @author tang
 *
 */
public class StringToDoubleConverter implements StringToNumberConverter<Double> {

	@Override
	public Double convert(String s) throws NumberFormatException {
		return Double.parseDouble(s);
	}

}
