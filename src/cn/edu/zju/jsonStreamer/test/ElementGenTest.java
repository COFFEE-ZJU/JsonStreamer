package cn.edu.zju.jsonStreamer.test;

import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.JsonSchema;
import cn.edu.zju.jsonStreamer.utils.JsonSchemaUtils;
import cn.edu.zju.jsonStreamer.utils.RandomJsonGenerator;

public class ElementGenTest {
	public void test() throws SystemErrorException{
		JsonSchema schema = JsonSchemaUtils.getSchemaByWrapperName("testSchema2");
		RandomJsonGenerator rjg = new RandomJsonGenerator(schema);
		System.out.println(rjg.generateRandomElement());
	}
	
	public static void main(String[] args) throws SystemErrorException {
		new ElementGenTest().test();
	}
}
