package cn.edu.zju.jsonStreamer.test;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import cn.edu.zju.jsonStreamer.test.GsonTest.Eq1;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

public class SerializableTest {
	public class MyObj implements Serializable{
		JsonElement jele;
		public MyObj(JsonElement jele){
			this.jele = jele;
		}
	}
	public class Eq1{
		int id;
		String str;
		public Eq1(int id, String str){
			this.id = id;
			this.str = str;
		}
	}
	
	public void test1() throws IOException{
		Eq1 eq1 = new Eq1(2, "test");
		JsonElement jele = new Gson().toJsonTree(eq1);
		MyObj myObj = new MyObj(jele);
		FileOutputStream fs = new FileOutputStream("foo.ser");  
		ObjectOutputStream os = new ObjectOutputStream(fs); 
		os.writeObject(myObj);  

	}
	
	public static void main(String[] args) throws IOException {
		new SerializableTest().test1();
	}

}
