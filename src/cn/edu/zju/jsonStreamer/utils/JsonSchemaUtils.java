package cn.edu.zju.jsonStreamer.utils;

import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;
import java.util.Map.Entry;


import cn.edu.zju.jsonStreamer.constants.Constants;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.JsonSchema;
import cn.edu.zju.jsonStreamer.wrapper.Wrapper;
import cn.edu.zju.jsonStreamer.wrapper.WrapperManager;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;


public class JsonSchemaUtils {
//	static class JsonSchemaQuery{
//		String type;
//		String wrapper_name;
//		
//		public JsonSchemaQuery(String wrapperName){
//			type = "get_schema";
//			wrapper_name = wrapperName;
//		}
//	}
//	
//	static class SchemaFormat{
//		JsonValueType type;
//		Object properties;
//		SchemaFormat items;
//	}
	
	public static JsonSchema getSchemaByWrapperName(String wrapperName) throws SystemErrorException{
		Wrapper wrapper = WrapperManager.getWrapperByName(wrapperName);
		return getSchemaBySchemaName(wrapper.schema_name);
	}
	
	public static JsonSchema getSchemaBySchemaName(String schemaName) throws SystemErrorException{
		File file = new File(Constants.SCHEMA_FILE_PATH + schemaName + ".txt");
		Long fileLengthLong = file.length();
		byte[] fileContent = new byte[fileLengthLong.intValue()];
		try {
			FileInputStream inputStream = new FileInputStream(file);
			inputStream.read(fileContent);
			inputStream.close();
		} catch (Exception e) {
			throw new SystemErrorException("no such schema!");
		}
		String jsonString = new String(fileContent);
		
		Object o = Constants.gson.fromJson(jsonString, Object.class);
		
		//System.out.println(parseSchema(toSchemaFormat(o)));
		return Constants.gson.fromJson(jsonString, JsonSchema.class);
	}
	
	public static boolean validate(JsonElement ele, JsonSchema schema){
		switch (schema.getType()) {
		case ARRAY:
			if(! ele.isJsonArray()) return false;
			else{
				Iterator<JsonElement> it = ele.getAsJsonArray().iterator();
				while(it.hasNext()){
					if(! validate(it.next(), schema.items)) return false;
				}
				return true;
			}
			
		case OBJECT:
			if(! ele.isJsonObject()) return false;
			else{
				JsonObject obj = ele.getAsJsonObject();
				Iterator<Entry<String, JsonSchema> > it = schema.properties.entrySet().iterator();
				Entry<String, JsonSchema> ent;
				while(it.hasNext()){
					ent = it.next();
					if(! obj.has(ent.getKey())) return false;
					if(! validate(obj.get(ent.getKey()), ent.getValue())) return false;
				}
				return true;
			}
			
		case NULL:
			if(! ele.isJsonNull()) return false;
			else return true;
			
		case BOOLEAN:
			if(! ele.isJsonPrimitive()) return false;
			else return ele.getAsJsonPrimitive().isBoolean();
			
		case INTEGER:
			if(! ele.isJsonPrimitive()) return false;
			else{
				JsonPrimitive jp = ele.getAsJsonPrimitive();
				if(! jp.isNumber()) return false;
				else return ! ele.toString().contains(".");
			}
		case NUMBER:
			if(! ele.isJsonPrimitive()) return false;
			else return ele.getAsJsonPrimitive().isNumber();
		case STRING:
			if(! ele.isJsonPrimitive()) return false;
			else return ele.getAsJsonPrimitive().isString();
			
		default:
			return false;
		}
	}
	
//	private static JsonSchema parseSchema(SchemaFormat sf){
//		JsonSchema js = new JsonSchema(sf.type);
//		if(sf.type == JsonValueType.ARRAY){
//			js.items = parseSchema(sf.items);
//		}
//		else if(sf.type == JsonValueType.OBJECT){
//			Map<String, Object> map = (Map<String, Object>)sf.properties;
//			Iterator<Entry<String, Object> > it = map.entrySet().iterator();
//			Entry<String, Object> ent;
//			SchemaFormat tempsf;
//			while(it.hasNext()){
//				ent = it.next();
//				//System.out.println(ent.getKey());
//				tempsf = toSchemaFormat(ent.getValue());
//				js.properties.put(ent.getKey(), parseSchema(tempsf));
////				if(type == JsonValueType.OBJECT) s.objectNameToSchema.put(ent.getKey(), parseSchema(sf.properties));
////				else if(type == JsonValueType.ARRAY){
////					SchemaFormat sf2 = (SchemaFormat)sf.items;
////					JsonValueType type2 = stringToJsonValueType.get(sf2.type);
////					s.arrayNameToType.put(ent.getKey(), type2);
////					if(type2 == JsonValueType.OBJECT) s.objectNameToSchema.put(ent.getKey(), parseSchema(sf2.properties));
////					else if(type2 == JsonValueType.ARRAY) throw new SemanticErrorException("array of array not supported yet");
////				}
//			}
//		}
//		return js;
//	}
//	
//	private static SchemaFormat toSchemaFormat(Object o){
//		Map<String, Object> map = (Map<String, Object>)o;
//		SchemaFormat sf = new SchemaFormat();
//		Iterator<Entry<String, Object> > it = map.entrySet().iterator();
//		Entry<String, Object> ent;
//		while(it.hasNext()){
//			ent = it.next();
//			switch (ent.getKey()) {
//			case "type":
//				sf.type = Constants.stringToJsonValueType.get((String)ent.getValue());
//				break;
//				
//			case "properties":
//				sf.properties = ent.getValue();
//				break;
//				
//			case "items":
//				sf.items = toSchemaFormat(ent.getValue());
//				break;
//
//			default:
//				break;
//			}
//		}
//		
//		return sf;
//	}
}
