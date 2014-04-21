package test;

import java.util.Iterator;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

public class JsonArrayTest {
	public void test(){
		JsonArray array = new JsonArray();
		JsonElement ele = new JsonPrimitive(10003);
		array.add(ele);
		Iterator<JsonElement> it = array.iterator();
		while(it.hasNext()){
			System.out.println(it.next());
		}
	}
	
	public static void main(String[] args) {
		new JsonArrayTest().test();
	}
}
