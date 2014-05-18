package cn.edu.zju.jsonStreamer.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
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
	
	public void test1(){
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
	
	@SuppressWarnings("deprecation")
	public void test2(){
		Gson gs = new Gson();
		JsonElement ele = new JsonNull();
		System.out.println(ele.isJsonNull());
	}
	
	class Eq1{
		int id;
		String str;
		public Eq1(int id, String str){
			this.id = id;
			this.str = str;
		}
	}
	class Eq2{
		
		String str;
		int id;
		public Eq2(int id, String str){
			this.id = id;
			this.str = str;
		}
	}
	
	public void test3(){
		
		Gson gs = new Gson();
		Eq1 eq1 = new Eq1(2, "test");
		Eq2 eq2 = new Eq2(2, "test");
//		String gstring1 = gs.toJson(1.00);
//		String gstring2 = gs.toJson(1);
		String gstring1 = gs.toJson(eq1);
		String gstring2 = gs.toJson(eq2);
		System.out.println(gstring1);
		System.out.println(gstring2);
		Map<JsonElement, Integer> mapper = new HashMap<JsonElement, Integer>();
		
		JsonElement ele1,ele2;
		ele1 = gs.fromJson(gstring1, JsonElement.class);
		ele2 = gs.fromJson(gstring2, JsonElement.class);
		
		mapper.put(ele1, 1);
		mapper.put(ele2, 2);
		System.out.println(mapper.get(ele1));
//		ele1.getAsJsonObject().add("new", new JsonPrimitive(111));
		System.out.println(ele1.equals(ele2));
		System.out.println(ele1.hashCode() + ",  " + ele2.hashCode());
	}
	
	public void test4(){
		Queue<Integer> queue = new LinkedList<Integer>();
		queue.add(1);
		queue.add(2);
		queue.add(3);
		queue.add(4);
		Iterator<Integer> it = queue.iterator();
		while(it.hasNext()) System.out.println(it.next());
	}
	
	public static void main(String[] args) {
		
		new GsonTest().test4();
	}
}
