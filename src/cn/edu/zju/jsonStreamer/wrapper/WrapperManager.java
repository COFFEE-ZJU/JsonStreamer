package cn.edu.zju.jsonStreamer.wrapper;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Constructor;

import cn.edu.zju.jsonStreamer.IO.input.JStreamInput;
import cn.edu.zju.jsonStreamer.IO.input.TestStreamInput;
import cn.edu.zju.jsonStreamer.constants.Constants;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.JsonSchema;
import cn.edu.zju.jsonStreamer.utils.JsonSchemaUtils;


public class WrapperManager {
	public static Wrapper getWrapperByName(String wrapperName) throws SystemErrorException{
		File file = new File(Constants.WRAPPER_FILE_PATH + wrapperName + ".txt");
		Long fileLengthLong = file.length();
		byte[] fileContent = new byte[fileLengthLong.intValue()];
		try {
			FileInputStream inputStream = new FileInputStream(file);
			inputStream.read(fileContent);
			inputStream.close();
		} catch (Exception e) {
			throw new SystemErrorException("no such wrapper!");
		}
		
		String jsonString = new String(fileContent);
		Wrapper wrapper = Constants.gson.fromJson(jsonString, Wrapper.class);
		
		
		return wrapper;
	}
	
	public static JStreamInput getStreamInputByWrapperForTest(String wrapperName) throws SystemErrorException{
		JsonSchema schema = JsonSchemaUtils.getSchemaBySchemaName(wrapperName);
		return new TestStreamInput(schema);
	}
	
	public static JStreamInput getStreamInputByWrapper(String wrapperName) throws SystemErrorException{
		Wrapper wrapper = getWrapperByName(wrapperName);
		try {
			Class<?> c = Class.forName("cn.edu.zju.jsonStreamer.IO.input."+wrapper.implement_class);
			Constructor<?> constructor = c.getConstructor();
			Object o = constructor.newInstance();
//			Constructor<?> constructor = c.getConstructor(JsonSchema.class);
//			Object o = constructor.newInstance(JsonSchemaUtils.getSchemaBySchemaName(wrapper.schema_name));
			return (JStreamInput)o;
		} catch (Exception e) {
			throw new SystemErrorException(e);
		}
		
	}
}
