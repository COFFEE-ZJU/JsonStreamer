package constants;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

public class Constants {
	public enum OperatorState {READY, SUSPEND};
	
	public enum CondType {GT, GE, EQ, LE, LT, NOT, NE, BOOL, AND, OR};
	public enum ExprType {ID, INT, NUMBER, BOOL, NULL, STRING, ADD, SUB, DIV, MUL, MOD, AGGREGATION};
	
	public static Gson gson = new Gson();
	public static String schemaFilePath = "schemas/";
	public static class WindowUnit {
		public static final String NOW = "now";
		public static final String UNBOUNDED = "unbounded";
		public static final String SECONDS = "seconds";
		public static final String MINUTES = "minutes";
		public static final String HOURS = "hours";
		public static final String DAYS = "days";
	}
	
	public static class JsonExprType {
		public static final String ID = "id";
		public static final String INT = "int";
		public static final String NUMBER = "number";
		public static final String BOOL = "bool";
		public static final String NULL = "null";
		public static final String STRING = "string";
		public static final String ADD = "add";
		public static final String SUB = "sub";
		public static final String DIV = "div";
		public static final String MUL = "mul";
		public static final String MOD = "mod";
		public static final String AGGREGATION = "aggregation";
	}
	
	public static class JsonConditionType {	//and, or, not, gt, ge, lt, le, eq, ne, bool
		public static final String AND = "and";
		public static final String OR = "or";
		public static final String NOT = "not";
		public static final String GT = "gt";
		public static final String GE = "ge";
		public static final String LT = "lt";
		public static final String LE = "le";
		public static final String EQ = "eq";
		public static final String NE = "ne";
		public static final String BOOL = "bool";
	}
	
	public static enum JsonValueType{ARRAY, BOOLEAN, INTEGER, NUMBER, NULL, OBJECT, STRING};
	public static enum JsonProjectionType{object, array, direct};
	public static enum JsonAttrSource{left, right, group_key_var, group_array};
	public static enum AggrFuncNames{sum, average, count};
	
	public static enum DataType {STREAM,RELATION};
	public static enum OperationType {JOIN, FILTER, TRANS, GROUP, STREAM, WINDOW, EXPAND, NULL};
	public static enum JsonOpType{
		root, error, 
		projection, leaf, join, groupby_aggregation, selection, expand, 
		rangewindow, rowwindow, partitionwindow,
		istream, rstream, dstream
		};
	
	public static String LT = "<";
	public static String LE = "<=";
	public static String EQ = "==";
	public static String GE = ">=";
	public static String GT = ">";
	public static String NE = "!=";
	
	public static boolean isStreamType(JsonOpType type){
		if(type == Constants.JsonOpType.leaf ||
    			type == Constants.JsonOpType.istream ||
    			type == Constants.JsonOpType.rstream ||
    			type == Constants.JsonOpType.dstream)
			return true;
		else
			return false;
	}
	
	public static Map<String, JsonValueType> stringToJsonValueType = new HashMap<String, Constants.JsonValueType>(){
		private static final long serialVersionUID = -3817390131714920532L;
		{
			put("array",JsonValueType.ARRAY);
			put("boolean",JsonValueType.BOOLEAN);
			put("integer",JsonValueType.INTEGER);
			put("number",JsonValueType.NUMBER);
			put("null",JsonValueType.NULL);
			put("object",JsonValueType.OBJECT);
			put("string",JsonValueType.STRING);
		}
	};
}
