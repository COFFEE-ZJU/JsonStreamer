package test;

import java.lang.reflect.Type;
import java.util.ArrayList;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.internal.LinkedTreeMap;

public class GsonTest {
	public class TestC{
		String test1;
		int testint;
		Type tt;
		public TestC(){
			Gson g = new Gson();
			tt = g.fromJson(g.toJson(test1).toUpperCase(), Type.class);
		}
		public TestC(String any, int num){
			test1 = any;
			testint = num;
		}
		public void trans(){
			Gson g = new Gson();
			tt = g.fromJson(g.toJson(test1).toUpperCase(), Type.class);
		}
	}
	public class TestC1{
		int testint;
		public TestC1(int num){
			testint = num;
		}
	}
	public enum Type {TA, TB};
	
	public GsonTest(){
		Gson gs = new Gson();
		int[] ints = new int[]{1,2,3};
		
		String gstring1 = gs.toJson(ints);
		String gstring2 = gs.toJson(new TestC("ta",100));
		String gstring3 = gs.toJson(100);
		String gstring4 = gs.toJson("TA");
		String gstring5 = gs.toJson(null);
		System.out.println(gstring5);
		Object o1 = gs.fromJson(gstring1, Object.class);
		Object o2 = gs.fromJson(gstring2, Object.class);
		Object o3 = gs.fromJson(gstring3, Object.class);
		Object o5 = gs.fromJson(gstring5, Object.class);
		Type tt = gs.fromJson(gstring4, Type.class);
		JsonElement je2 = gs.fromJson(gstring2, JsonElement.class);
		JsonElement je3 = gs.fromJson(gstring3, JsonElement.class);
		TestC c = gs.fromJson(gstring2, TestC.class);
		c.trans();
		TestC1 c1 = gs.fromJson(je2, TestC1.class);
		System.out.println(c1.testint);
		System.out.println(je2);
		System.out.println(je3.getAsBoolean());
		//System.out.println(((Double)o).intValue());
		System.out.println(o1.getClass() == ArrayList.class);
		System.out.println(o2.getClass() == LinkedTreeMap.class);
		System.out.println(o3.getClass());
		System.out.println(((LinkedTreeMap<String,Object>)o2).values().toArray()[0].getClass());
		System.out.println(tt);
		System.out.println(c.tt);
		System.out.println(o5.getClass());
	}
	
	public static void main(String[] args) {
		
		new GsonTest();
	}
}
