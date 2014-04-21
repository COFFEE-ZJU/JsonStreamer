package test;

import java.lang.reflect.Constructor;

public class ReflectTest {
	public ReflectTest(Integer val){
		System.out.println("im in ctr!!!");
	}
	public static void test(){
		String className = "test.ReflectTest";
		try {
			Class<?> c = Class.forName(className);
			Constructor<?> constructor = c.getConstructor(Integer.class);
			Object o = constructor.newInstance(123);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		ReflectTest.test();
	}
}
