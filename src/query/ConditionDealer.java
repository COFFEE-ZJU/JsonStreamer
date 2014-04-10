package query;

import constants.Constants.JsonCondType;
import json.Element;
import jsonAPI.JsonCondition;

public abstract class ConditionDealer {
	protected final JsonCondition condition;
	protected ConditionDealer(JsonCondition cond){
		condition = cond;
	}
	
	public abstract boolean deal(Element ele);
	
	public static ConditionDealer genConditionDealer(JsonCondition cond){
		switch (cond.condition_type) {
		case AND:
		case OR:
		case NOT:
			return new LogiDealer(cond);
		case BOOL:
			return new BoolDealer(cond);
		case EQ:
		case NE:
		case LT:
		case GT:
		case LE:
		case GE:
			return new CompareDealer(cond);
		}
		
		return null;
	}
}

class LogiDealer extends ConditionDealer{
	private final ConditionDealer leftDealer, rightDealer, dealer;
	protected LogiDealer(JsonCondition cond) {
		super(cond);
		if(cond.condition_type == JsonCondType.NOT){
			leftDealer = rightDealer = null;
			dealer = ConditionDealer.genConditionDealer(cond.condition);
		}
		else {
			leftDealer = ConditionDealer.genConditionDealer(cond.left_condition);
			rightDealer = ConditionDealer.genConditionDealer(cond.right_condition);
			dealer = null;
		}
	}

	@Override
	public boolean deal(Element ele) {
		switch (condition.condition_type) {
		case NOT:
			return ! dealer.deal(ele);
		case AND:
			return leftDealer.deal(ele) && rightDealer.deal(ele);
		case OR:
			return leftDealer.deal(ele) || rightDealer.deal(ele);
		}
		return false;
	}
	
}

class BoolDealer extends ConditionDealer{
	private final ExpressionDealer dealer;
	protected BoolDealer(JsonCondition cond) {
		super(cond);
		dealer = ExpressionDealer.genExpressionDealer(cond.bool_expression);
	}

	@Override
	public boolean deal(Element ele) {
		return dealer.deal(ele).jsonElement.getAsBoolean();
	}
	
}

class CompareDealer extends ConditionDealer{
	private final ExpressionDealer leftDealer, rightDealer;
	protected CompareDealer(JsonCondition cond) {
		super(cond);
		leftDealer = ExpressionDealer.genExpressionDealer(cond.left_expression);
		rightDealer = ExpressionDealer.genExpressionDealer(cond.right_expression);
	}

	@Override
	public boolean deal(Element ele) {
		Element left,right;
		switch (condition.condition_type) {
		case EQ:
			left = leftDealer.deal(ele);
			right = rightDealer.deal(ele);
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
		case NE:
			left = leftDealer.deal(ele);
			right = rightDealer.deal(ele);
			switch (left.type) {
			case BOOLEAN:
				return left.jsonElement.getAsBoolean() != right.jsonElement.getAsBoolean();
			case INTEGER:
			case NUMBER:
				return left.jsonElement.getAsDouble() != right.jsonElement.getAsDouble();
			case NULL:
				return left.jsonElement.isJsonNull() != left.jsonElement.isJsonNull();
			case STRING:
				return ! left.jsonElement.getAsString().equals(right.jsonElement.getAsString());
			case ARRAY: //TODO
			case OBJECT: //TODO

			default:
				return false;
			}
		case LT:
			left = rightDealer.deal(ele);		//the opposite way, reduce code redundancy
			right = leftDealer.deal(ele);
		case GT:
			left = leftDealer.deal(ele);
			right = rightDealer.deal(ele);
			switch (left.type) {
			case INTEGER:
			case NUMBER:
				return left.jsonElement.getAsDouble() > right.jsonElement.getAsDouble();
			case STRING:
				return left.jsonElement.getAsString().compareTo(right.jsonElement.getAsString()) > 0;

			default:
				return false;
			}
			
		case LE:
			left = rightDealer.deal(ele);		//the opposite way, reduce code redundancy
			right = leftDealer.deal(ele);
		case GE:
			left = leftDealer.deal(ele);
			right = rightDealer.deal(ele);
			switch (left.type) {
			case INTEGER:
			case NUMBER:
				return left.jsonElement.getAsDouble() >= right.jsonElement.getAsDouble();
			case STRING:
				return left.jsonElement.getAsString().compareTo(right.jsonElement.getAsString()) >= 0;

			default:
				return false;
			}
		}
		return false;
	}
	
}