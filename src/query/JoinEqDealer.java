package query;

import json.Element;
import jsonAPI.JsonExpression;

public class JoinEqDealer {
	private ExpressionDealer leftDealer, rightDealer;
	
	public JoinEqDealer(JsonExpression left, JsonExpression right){
		leftDealer = ExpressionDealer.genExpressionDealer(left);
		leftDealer = ExpressionDealer.genExpressionDealer(right);
	}
	
	public boolean deal(Element leftEle, Element rightEle){
		Element left = leftDealer.deal(leftEle);
		Element right = rightDealer.deal(rightEle);
		
		switch (left.type) {
		case BOOLEAN:
			return left.jsonElement.getAsBoolean() == right.jsonElement.getAsBoolean();
		case INTEGER:
		case NUMBER:
			return left.jsonElement.getAsDouble() == right.jsonElement.getAsDouble();
		case NULL:
			return left.jsonElement.isJsonNull() == left.jsonElement.isJsonNull();
		case STRING:
			return left.jsonElement.getAsString().equals(right.jsonElement.getAsString());
		case ARRAY: //TODO
		case OBJECT: //TODO

		default:
			return false;
		}
	}
}
