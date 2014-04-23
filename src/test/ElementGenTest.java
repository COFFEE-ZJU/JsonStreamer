package test;

import constants.SystemErrorException;
import utils.JsonSchemaUtils;
import utils.RandomJsonGenerator;
import json.JsonSchema;

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
