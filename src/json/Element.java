package json;

import java.util.ArrayList;
import java.util.Iterator;

import jsonAPI.JsonExpression;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import constants.Constants;

public class Element {
	public final JsonElement element;
	public Element(String jsonString){
		element = Constants.gson.fromJson(jsonString, JsonElement.class);
	}
	public JsonElement getValue(Object[] path){
		return getValue(path, 0, element);
	}
	public JsonElement getValue(JsonExpression expr){
		return null; //TODO
	}
	
	private JsonElement getValue(Object[] path, int index, JsonElement ele){
		if(path.length == index) return ele;
		Object pathObj = path[index];
		if(pathObj.getClass() == String.class) return getValue(path, index+1, ele.getAsJsonObject().get((String)pathObj));
		else if(pathObj.getClass() == Double.class){
			int ind = ((Double)pathObj).intValue();
			if(ind != -1) return getValue(path, index+1, ele.getAsJsonArray().get(ind));
			else{
				Iterator<JsonElement> it = ele.getAsJsonArray().iterator();
				JsonArray array = new JsonArray();
				while(it.hasNext()){
					array.add(getValue(path, index+1, it.next()));
				}
				return array;
			}
		}
		else if(pathObj.getClass() == ArrayList.class){
			ArrayList<Integer> list = (ArrayList<Integer>)pathObj;
			//if(ele.getAsJsonArray().size() <= list.get(1)) throw new IndexOutOfBoundsException();
			Iterator<JsonElement> it = ele.getAsJsonArray().iterator();
			int i = 0, low = list.get(0), up = list.get(1);
			JsonArray array = new JsonArray();
			while(it.hasNext()){
				if(i>=low && i<= up) array.add(getValue(path, index+1, it.next()));
				else it.next();
			}
			return array;
		}
		else return null;
		
	}
	
	
}
