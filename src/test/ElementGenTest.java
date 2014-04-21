package test;

import utils.JsonSchemaUtils;
import utils.RandomJsonGenerator;
import json.JsonSchema;

public class ElementGenTest {
	public void test(){
		JsonSchema schema = JsonSchemaUtils.getSchemaByWrapperName("testSchema2");
		RandomJsonGenerator rjg = new RandomJsonGenerator(schema);
		System.out.println(rjg.generateRandomElement());
	}
	
	public static void main(String[] args) {
		new ElementGenTest().test();
	}
}
