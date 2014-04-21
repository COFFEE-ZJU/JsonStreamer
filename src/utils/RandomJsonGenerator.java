package utils;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import json.Element;
import json.JsonSchema;

public class RandomJsonGenerator {
	private JsonSchema schema;
	private static final int stringMaxLength = 20;
	private static final int arrayMaxLenghth = 20;
	private static final int intMax = 1000000;
	private static final double doubleMax = 1000000.0;
	public static final Random random = new Random();
	
	public RandomJsonGenerator(JsonSchema schema){
		this.schema = schema;
	}
	
	public Element generateRandomElement(){
		return new Element(generateBySchema(schema), schema.getType());
	}
	
	@SuppressWarnings("deprecation")
	private static JsonElement generateBySchema(JsonSchema schema){
		switch (schema.getType()) {
		case ARRAY:
			int arrayLen = random.nextInt(arrayMaxLenghth)+1;
			JsonArray array = new JsonArray();
			for(int i = 0; i<arrayLen; i++){
				array.add(generateBySchema(schema.items));
			}
			return array;
			
		case OBJECT:
			Iterator<Entry<String, JsonSchema> > it = schema.properties.entrySet().iterator();
			Entry<String, JsonSchema> ent;
			JsonObject obj = new JsonObject();
			while(it.hasNext()){
				ent = it.next();
				obj.add(ent.getKey(), generateBySchema(ent.getValue()));
			}
			return obj;
			
		case BOOLEAN:
			return new JsonPrimitive(random.nextBoolean());
			
		case INTEGER:
			return new JsonPrimitive(random.nextInt(intMax));
			
		case NUMBER:
			return new JsonPrimitive((random.nextDouble()*2.0-1.0) * doubleMax);
			
		case NULL:
			return new JsonNull();
			
		case STRING:
			return new JsonPrimitive(getRandomString(stringMaxLength));
		}
		
		return null;
	}
	
	private static String getRandomString(int length) {
	    String base = "abcdefghijklmnopqrstuvwxyz0123456789 ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	    StringBuffer sb = new StringBuffer();   
	    for (int i = 0; i < length; i++) {   
	        int number = random.nextInt(base.length());   
	        sb.append(base.charAt(number));   
	    }
	    return sb.toString();   
	 }   

}
