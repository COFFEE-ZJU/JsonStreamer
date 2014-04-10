package operators;

import query.ConditionDealer;

import json.Element;
import jsonAPI.JsonCondition;
import jsonAPI.JsonQueryTree;

public class SelectionOperator extends Operator{
	private final JsonCondition condition;
	private final ConditionDealer dealer;
	
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
		return dealer.deal(ele);
	}
	
	public SelectionOperator(JsonQueryTree tree){
		super(tree);
		this.condition = tree.selection_condition;
		dealer = ConditionDealer.genConditionDealer(condition);
	}
	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
	}

}
