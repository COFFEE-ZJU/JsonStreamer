package operators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonElement;
import com.google.gson.annotations.Expose;
import com.sun.xml.internal.ws.server.sei.SEIInvokerTube;

import constants.Constants;
import constants.Constants.AggrFuncNames;
import constants.Constants.CondType;
import constants.Constants.ExprType;
import constants.Constants.JsonAttrSource;

import json.Element;
import jsonAPI.JsonCondition;
import jsonAPI.JsonExpression;
import jsonAPI.JsonProjection;
import jsonAPI.JsonQueryTree;

public class SelectionOperator extends Operator{
	private final JsonCondition condition;
	
//	private static final Map<String, CondType> stringToCondType = new HashMap<String, CondType>(){{
//		put("and", CondType.AND);
//		put("or", CondType.OR);
//		put("not", CondType.NOT);
//		put("gt", CondType.GT);
//		put("ge", CondType.GE);
//		put("lt", CondType.LT);
//		put("le", CondType.LE);
//		put("eq", CondType.EQ);
//		put("ne", CondType.NE);
//		put("bool", CondType.BOOL);
//	}};
//	
//	private static final Map<String, ExprType> stringToExprType = new HashMap<String, ExprType>(){{
//		put("id", ExprType.ID);	//id, int, number, bool, null, string, add, sub, div, mul, mod, aggregation;
//		put("int", ExprType.INT);
//		put("number", ExprType.NUMBER);
//		put("bool", ExprType.BOOL);
//		put("null", ExprType.NULL);
//		put("string", ExprType.STRING);
//		put("add", ExprType.ADD);
//		put("sub", ExprType.SUB);
//		put("div", ExprType.DIV);
//		put("mul", ExprType.MUL);
//		put("mod", ExprType.MOD);
//		put("aggregation", ExprType.AGGREGATION);
//	}};
//	
//	private class FastCondition{
//		public CondType type = null;
//		
//		public FastCondition condition = null;
//		public FastCondition left_condition = null;
//		public FastCondition right_condition = null;
//		public FastExpression left_expression = null;
//		public FastExpression right_expression = null;
//		public FastExpression bool_expression = null;
//		
//		public FastCondition(JsonCondition cond){
//			type = stringToCondType.get(cond.condition_type);
//			switch (type) {
//			case AND:
//			case OR:
//				left_condition = new FastCondition(cond.left_condition);
//				right_condition = new FastCondition(cond.right_condition);
//				break;
//				
//			case NOT:
//				this.condition = new FastCondition(cond.condition);
//				break;
//				
//			case GT:
//			case GE:
//			case LT:
//			case LE:
//			case EQ:
//			case NE:
//				left_expression = new FastExpression(cond.left_expression);
//				right_expression = new FastExpression(cond.right_expression);
//				break;
//				
//			case BOOL:
//				bool_expression = new FastExpression(cond.bool_expression);
//				break;
//
//			default:
//				break;
//			}
//		}
//	}
//	
//	private class FastExpression {
//		public ExprType type = null;		//id, int, number, bool, null, string, add, sub, div, mul, mod, aggregation;
//		public AggrFuncNames aggregate_operation = null;
//		public JsonProjection aggregate_projection = null;
//		public JsonExpression left = null;
//		public JsonExpression right = null;
//		public List<Object> id_name = null;
//		public String string_value = null;
//		public Integer int_value = null;
//		public Double number_value = null;
//		public Boolean bool_value = null;
//		public JsonAttrSource attribute_source = null;		//for join operation only
//		
//		public FastExpression(JsonExpression expr){
//			type = stringToExprType.get(expr.expression_type);
//			attribute_source = expr.attribute_source;
//			switch (type) {
//			case ID:
//				break;
//
//			default:
//				break;
//			}
//		}
//	}
	
	
	private boolean satisfyCondition(Element ele, JsonCondition cond){
		JsonElement left,right;
		switch (condition.getType()) {
		case AND:
			return satisfyCondition(ele, cond.left_condition)&&satisfyCondition(ele, cond.right_condition);
		case OR:
			return satisfyCondition(ele, cond.left_condition)||satisfyCondition(ele, cond.right_condition);
		case NOT:
			return ! satisfyCondition(ele, cond.condition);
		case BOOL:
			return ele.getValue(cond.bool_expression).getAsBoolean();
		case EQ:
			left = ele.getValue(cond.left_expression);
			right = ele.getValue(cond.right_expression);
			switch (cond.left_expression.retSchema.type) {
			case BOOLEAN:
				return left.getAsBoolean() == right.getAsBoolean();
			case INTEGER:
			case NUMBER:
				return left.getAsDouble() == right.getAsDouble();
			case NULL:
				return left.isJsonNull() == left.isJsonNull();
			case STRING:
				return left.getAsString().equals(right.getAsString());
			case ARRAY: //TODO
			case OBJECT: //TODO

			default:
				return false;
			}
		case NE:
			left = ele.getValue(cond.left_expression);
			right = ele.getValue(cond.right_expression);
			switch (cond.left_expression.retSchema.type) {
			case BOOLEAN:
				return left.getAsBoolean() != right.getAsBoolean();
			case INTEGER:
			case NUMBER:
				return left.getAsDouble() != right.getAsDouble();
			case NULL:
				return left.isJsonNull() != left.isJsonNull();
			case STRING:
				return ! left.getAsString().equals(right.getAsString());
			case ARRAY: //TODO
			case OBJECT: //TODO

			default:
				return false;
			}
		case LT:
			left = ele.getValue(cond.right_expression);		//the opposite way, reduce code redundancy
			right = ele.getValue(cond.left_expression);
		case GT:
			left = ele.getValue(cond.left_expression);
			right = ele.getValue(cond.right_expression);
			switch (cond.left_expression.retSchema.type) {
			case INTEGER:
			case NUMBER:
				return left.getAsDouble() > right.getAsDouble();
			case STRING:
				return left.getAsString().compareTo(right.getAsString()) > 0;

			default:
				return false;
			}
			
		case LE:
			left = ele.getValue(cond.right_expression);		//the opposite way, reduce code redundancy
			right = ele.getValue(cond.left_expression);
		case GE:
			left = ele.getValue(cond.left_expression);
			right = ele.getValue(cond.right_expression);
			switch (cond.left_expression.retSchema.type) {
			case INTEGER:
			case NUMBER:
				return left.getAsDouble() >= right.getAsDouble();
			case STRING:
				return left.getAsString().compareTo(right.getAsString()) >= 0;

			default:
				return false;
			}

		default:
			break;
		}
		return false;
	}
	
	public SelectionOperator(JsonQueryTree tree){
		super(tree);
		this.condition = tree.selection_condition;
	}
	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
	}

}
