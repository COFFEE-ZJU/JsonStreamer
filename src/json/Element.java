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
}
