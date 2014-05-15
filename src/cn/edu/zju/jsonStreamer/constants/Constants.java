package cn.edu.zju.jsonStreamer.constants;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

public class Constants {
	public static enum ExecutionMode{LOCAL, STORM};
	public static final ExecutionMode EXECUTION_MODE = ExecutionMode.LOCAL;
	
	public static final int COMPILER_PORT = 3000;
	public static final int SCHEMA_SERVER_PORT = 2048;
	public static final int LISTEN_PORT = 2703;
	public static final int THREAD_POOL_NUM = 20;
	public static final int INPUT_STREAM_LENGTH = 1024;
	public static final int SERVER_ACCEPT_TIMEOUT = 1000;
	public static class ConnectionCmds{
		public static final String USER_PREFIX = "USER:";
		public static final String PW_PREFIX = "PW:";
		public static final String QUERY_START = "QUERY_START";
		public static final String QUERY_PREFIX = "QUERY:";
		public static final String QUERY_END = "QUERY_END";
		public static final String QUIT = "QUIT";
	}
	
	public static enum OperatorState {READY, SUSPEND};
	
	public static enum TimeUnitForNow {MILLISECOND, SECOND, MINUTE};
	public static final TimeUnitForNow TIME_UNIT_FOR_NOW = TimeUnitForNow.SECOND;	//configuration
	
	public static enum ElementMark {PLUS, MINUS, UNMARKED};
	public static enum JsonExprType {ID, INT, NUMBER, BOOL, NULL, STRING, ADD, SUB, DIV, MUL, MOD, AGGREGATION};
	public static enum JsonCondType {AND, OR, NOT, GT, GE, LT, LE, EQ, NE, BOOL};
	public static enum ErrorType {SEMANTIC_ERROR, SYNTAX_ERROR, SYSTEM_ERROR};
	
	public static Gson gson = new Gson();
	public static final String QUERY_FILE_PATH = "queries/";
	public static final String SCHEMA_FILE_PATH = "schemas/";
	public static final String WRAPPER_FILE_PATH = "wrappers/";
	public static final String RESULT_FILE_PATH = "resultFiles/";
	public static class WindowUnit {
		public static final String NOW = "now";
		public static final String UNBOUNDED = "unbounded";
		public static final String SECONDS = "seconds";
		public static final String MINUTES = "minutes";
		public static final String HOURS = "hours";
		public static final String DAYS = "days";
	}
	
//	public static class JsonConditionType {	//and, or, not, gt, ge, lt, le, eq, ne, bool
//		public static final String AND = "and";
//		public static final String OR = "or";
//		public static final String NOT = "not";
//		public static final String GT = "gt";
//		public static final String GE = "ge";
//		public static final String LT = "lt";
//		public static final String LE = "le";
//		public static final String EQ = "eq";
//		public static final String NE = "ne";
//		public static final String BOOL = "bool";
//	}
	
	public static enum JsonValueType{ARRAY, BOOLEAN, INTEGER, NUMBER, NULL, OBJECT, STRING};
	public static enum JsonProjectionType{OBJECT, ARRAY, DIRECT};
	public static enum JsonAttrSource{LEFT, RIGHT, GROUP_KEY_VAR, GROUP_ARRAY};
	public static enum AggrFuncNames{SUM, AVERAGE, COUNT};
	
	public static enum DataType {STREAM,RELATION};
	public static enum OperationType {JOIN, FILTER, TRANS, GROUP, STREAM, WINDOW, EXPAND, NULL};
	public static enum JsonOpType{
		ROOT, ERROR, 
		PROJECTION, LEAF, JOIN, GROUPBY_AGGREGATION, SELECTION, EXPAND, 
		RANGEWINDOW, ROWWINDOW, PARTITIONWINDOW,
		ISTREAM, RSTREAM, DSTREAM
		};
	
	public static final String LT = "<";
	public static final String LE = "<=";
	public static final String EQ = "==";
	public static final String GE = ">=";
	public static final String GT = ">";
	public static final String NE = "!=";
	
	public static boolean isStreamType(JsonOpType type){
		if(type == Constants.JsonOpType.LEAF ||
    			type == Constants.JsonOpType.ISTREAM ||
    			type == Constants.JsonOpType.RSTREAM ||
    			type == Constants.JsonOpType.DSTREAM)
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
	
	//for storm
	public static class StormFields{
		public static final String markedElement = "marked_element";
		public static final String timeStamp = "time_stamp";
		public static final String mark = "mark";
		public static final String id = "id";
		public static final String element = "element";
		public static final String groupKey = "group_key";
		public static final String sourceId = "source_id";
	}
	public static final String stormJedisSendQueue = "JsonStreamer_send_";
	public static final String stormJedisReceiveQueue = "JsonStreamer_receive_";
	
	private static String myIp = null;
	public static String getMyIp() throws SystemErrorException{
		if(myIp != null) return myIp;
		InetAddress addr = null;
		try {
			addr = InetAddress.getLocalHost();
			myIp = addr.getHostAddress().toString();
			return myIp;
		} catch (UnknownHostException e) {
			throw new SystemErrorException(e);
		}
	};
}
