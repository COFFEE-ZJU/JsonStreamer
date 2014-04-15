package query;

import java.util.ArrayList;
import java.util.Iterator;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonPrimitive;

import constants.Constants.AggrFuncNames;
import constants.Constants.JsonAttrSource;
import constants.Constants.JsonValueType;

import json.Element;
import json.JsonSchema;
import jsonAPI.JsonExpression;

public abstract class ExpressionDealer {
	protected final JsonExpression expression;
	protected ExpressionDealer(JsonExpression expr){
		expression = expr;
	}
	
	public Element deal(Element ele){
		return deal(ele, null);
	}
	public abstract Element deal(Element ele, Element rightEle);
	
	public static ExpressionDealer genExpressionDealer(JsonExpression expr){
		switch (expr.expression_type) {
		case INT:
		case BOOL:
		case STRING:
		case NUMBER:
		case NULL:
			return new PrimitiveDealer(expr);
		case ID:
			return new IdDealer(expr);
		case ADD:
		case SUB:
		case MUL:
		case DIV:
		case MOD:
			return new CalcDealer(expr);
		case AGGREGATION:
			return new AggrDealer(expr);
		}
		return null;
	}
}

class PrimitiveDealer extends ExpressionDealer{
	
	public PrimitiveDealer(JsonExpression expr){
		super(expr);
	}
	@SuppressWarnings("deprecation")
	@Override
	public Element deal(Element ele, Element rightEle) {
		switch (expression.expression_type) {
		case INT:
			return new Element(new JsonPrimitive(expression.int_value), JsonValueType.INTEGER);
		case BOOL:
			return new Element(new JsonPrimitive(expression.bool_value), JsonValueType.BOOLEAN);
		case STRING:
			return new Element(new JsonPrimitive(expression.string_value), JsonValueType.STRING);
		case NUMBER:
			return new Element(new JsonPrimitive(expression.number_value), JsonValueType.NUMBER);
		case NULL:
			return new Element(new JsonNull(), JsonValueType.NULL);

		default:
			break;
		}
		return null;
	}
}

class IdDealer extends ExpressionDealer{
	private final Boolean isLeftOrGroupArray;
	protected IdDealer(JsonExpression expr) {
		super(expr);
		if(expr.attribute_source == null) isLeftOrGroupArray = null;
		else if(expr.attribute_source == JsonAttrSource.left || expr.attribute_source == JsonAttrSource.group_array)
			isLeftOrGroupArray = true;
		else isLeftOrGroupArray = false;
	}

	@Override
	public Element deal(Element ele, Element rightEle) {
		if(isLeftOrGroupArray == null || rightEle == null)		//not in join or groupby
			return new Element(getIdValue(expression.id_name.toArray(), 0, ele.jsonElement), expression.retSchema.type);
		
		if(isLeftOrGroupArray == false)		//use left source in join or group array in groupby
			return new Element(getIdValue(expression.id_name.toArray(), 0, rightEle.jsonElement), expression.retSchema.type);
		else								//use right source in join or group key var in groupby
			return new Element(getIdValue(expression.id_name.toArray(), 0, ele.jsonElement), expression.retSchema.type);
	}

	private static JsonElement getIdValue(Object[] path, int index, JsonElement ele){
		if(path.length == index) return ele;
		Object pathObj = path[index];
		if(pathObj.getClass() == String.class) return getIdValue(path, index+1, ele.getAsJsonObject().get((String)pathObj));
		else if(pathObj.getClass() == Double.class){
			int ind = ((Double)pathObj).intValue();
			if(ind != -1) return getIdValue(path, index+1, ele.getAsJsonArray().get(ind));
			else{
				Iterator<JsonElement> it = ele.getAsJsonArray().iterator();
				JsonArray array = new JsonArray();
				while(it.hasNext()){
					array.add(getIdValue(path, index+1, it.next()));
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
				if(i>=low && i<= up) array.add(getIdValue(path, index+1, it.next()));
				else it.next();
			}
			return array;
		}
		else return null;
		
	}
}

class CalcDealer extends ExpressionDealer{
	private final ExpressionDealer leftDealer, rightDealer;
	protected CalcDealer(JsonExpression expr) {
		super(expr);
		leftDealer = ExpressionDealer.genExpressionDealer(expr.left);
		rightDealer = ExpressionDealer.genExpressionDealer(expr.right);
	}

	@Override
	public Element deal(Element ele, Element rightEle) {
		Element left, right;
		switch (expression.expression_type) {
		case ADD:
			left = leftDealer.deal(ele, rightEle);
			right = rightDealer.deal(ele, rightEle);
			if(left.type == JsonValueType.INTEGER)
				return new Element(new JsonPrimitive(left.jsonElement.getAsInt() + right.jsonElement.getAsInt()), JsonValueType.INTEGER);
			else
				return new Element(new JsonPrimitive(left.jsonElement.getAsDouble() + right.jsonElement.getAsDouble()), JsonValueType.NUMBER);
		case SUB:
			left = leftDealer.deal(ele, rightEle);
			right = rightDealer.deal(ele, rightEle);
			if(left.type == JsonValueType.INTEGER)
				return new Element(new JsonPrimitive(left.jsonElement.getAsInt() - right.jsonElement.getAsInt()), JsonValueType.INTEGER);
			else
				return new Element(new JsonPrimitive(left.jsonElement.getAsDouble() - right.jsonElement.getAsDouble()), JsonValueType.NUMBER);
		case MUL:
			left = leftDealer.deal(ele, rightEle);
			right = rightDealer.deal(ele, rightEle);
			if(left.type == JsonValueType.INTEGER)
				return new Element(new JsonPrimitive(left.jsonElement.getAsInt() * right.jsonElement.getAsInt()), JsonValueType.INTEGER);
			else
				return new Element(new JsonPrimitive(left.jsonElement.getAsDouble() * right.jsonElement.getAsDouble()), JsonValueType.NUMBER);
		case DIV:
			left = leftDealer.deal(ele, rightEle);
			right = rightDealer.deal(ele, rightEle);
			if(left.type == JsonValueType.INTEGER)
				return new Element(new JsonPrimitive(left.jsonElement.getAsInt() / right.jsonElement.getAsInt()), JsonValueType.INTEGER);
			else
				return new Element(new JsonPrimitive(left.jsonElement.getAsDouble() / right.jsonElement.getAsDouble()), JsonValueType.NUMBER);
		case MOD:
			left = leftDealer.deal(ele, rightEle);
			right = rightDealer.deal(ele, rightEle);
			return new Element(new JsonPrimitive(left.jsonElement.getAsInt() % right.jsonElement.getAsInt()), JsonValueType.INTEGER);

		default:
			break;
		}
		return null;
	}
}

class AggrDealer extends ExpressionDealer{
	private final ProjectionDealer projDealer;
	protected AggrDealer(JsonExpression expr) {
		super(expr);
		projDealer = ProjectionDealer.genProjectionDealer(expr.aggregate_projection);
	}

	@Override
	public Element deal(Element ele, Element rightEle) {
		JsonArray array = projDealer.deal(ele, rightEle).jsonElement.getAsJsonArray();
		int size = array.size();
		JsonPrimitive jp;
		if(expression.aggregate_operation == AggrFuncNames.count) 
			jp = new JsonPrimitive(size);
		else{
			Iterator<JsonElement> it = array.iterator();
			double sum = 0;
			while(it.hasNext())
				sum += it.next().getAsDouble();
			
			if(expression.aggregate_operation == AggrFuncNames.sum)
				jp = new JsonPrimitive(sum);
			else jp = new JsonPrimitive(sum/size);
		}
		
		return new Element(jp, expression.retSchema.type);
	}
	
}
