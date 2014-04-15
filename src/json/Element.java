package json;

import com.google.gson.JsonElement;
import constants.Constants.JsonValueType;

public class Element {
	public final JsonValueType type;
	public final JsonElement jsonElement;
//	public Element(String jsonString){
//		element = Constants.gson.fromJson(jsonString, JsonElement.class);
//	}
	public Element(JsonElement ele, JsonValueType type){
		jsonElement = ele;
		this.type = type;
	}
	
	@Override
	public int hashCode(){
		return jsonElement.hashCode();
	}
	@Override
	public boolean equals(Object object){
		if(!(object instanceof Element)) return false;
		Element otherEle = (Element) object;
		return this.jsonElement.equals(otherEle.jsonElement);
//		if(this.type != otherEle.type) return false;
//		
//		switch (type) {
//		case BOOLEAN:
//			return this.jsonElement.getAsBoolean() == otherEle.jsonElement.getAsBoolean();
//		case INTEGER:
//		case NUMBER:
//			return this.jsonElement.getAsDouble() == otherEle.jsonElement.getAsDouble();
//		case NULL:
//			return this.jsonElement.isJsonNull() == otherEle.jsonElement.isJsonNull();
//		case STRING:
//			return this.jsonElement.getAsString().equals(otherEle.jsonElement.getAsString());
//		case ARRAY: 
//			JsonArray array1 = this.jsonElement.getAsJsonArray();
//			JsonArray array2 = otherEle.jsonElement.getAsJsonArray();
//			if(array1.size() != array2.size()) return false;
//			Iterator<JsonElement> it1 = array1.iterator();
//			Iterator<JsonElement> it2 = array2.iterator();
//			JsonElement ele1, ele2;
//			while(it1.hasNext()){
//				ele1 = it1.next();
//				ele2 = it2.next();
//				
//			}
//		case OBJECT: //TODO
//
//		default:
//			return false;
//		}
	}
}
