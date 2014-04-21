package test;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;

import json.JsonSchema;
import jsonAPI.JsonQueryTree;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class APITest {
	
	public void test1(){
		File file = new File("APITest1.txt");
		Long fileLengthLong = file.length();
		byte[] fileContent = new byte[fileLengthLong.intValue()];
		try {
		        FileInputStream inputStream = new FileInputStream(file);
		        inputStream.read(fileContent);
		        inputStream.close();
		} catch (Exception e) {
			// TODO: handle exception
		}
		String api = new String(fileContent);
		
		//System.out.println(api);
		List<JsonQueryTree> jqt = new Gson().fromJson(api, new TypeToken<List<JsonQueryTree> >(){}.getType());
		
		System.out.println(new Gson().toJson(jqt.get(0)));
	}
	
	public void test2(){
		File file = new File("APITest2.txt");
		Long fileLengthLong = file.length();
		byte[] fileContent = new byte[fileLengthLong.intValue()];
		try {
		        FileInputStream inputStream = new FileInputStream(file);
		        inputStream.read(fileContent);
		        inputStream.close();
		} catch (Exception e) {
			// TODO: handle exception
		}
		String api = new String(fileContent);
		
		JsonSchema s = new Gson().fromJson(api, JsonSchema.class);
		
		System.out.println(s.properties.values());
	}

	public static void main(String[] args) {
		new APITest().test2();
	}

}
