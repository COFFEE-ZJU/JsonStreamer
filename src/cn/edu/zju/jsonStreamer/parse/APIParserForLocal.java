package cn.edu.zju.jsonStreamer.parse;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import cn.edu.zju.jsonStreamer.IO.output.JStreamOutput;
import cn.edu.zju.jsonStreamer.constants.JsonStreamerException;
import cn.edu.zju.jsonStreamer.constants.SemanticErrorException;
import cn.edu.zju.jsonStreamer.constants.SyntaxErrorException;
import cn.edu.zju.jsonStreamer.constants.SystemErrorException;
import cn.edu.zju.jsonStreamer.json.MarkedElement;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonError;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonQueryTree;
import cn.edu.zju.jsonStreamer.operators.AggregationOperator;
import cn.edu.zju.jsonStreamer.operators.DStreamOperator;
import cn.edu.zju.jsonStreamer.operators.ExpandOperator;
import cn.edu.zju.jsonStreamer.operators.IStreamOperator;
import cn.edu.zju.jsonStreamer.operators.JoinOperator;
import cn.edu.zju.jsonStreamer.operators.LeafOperator;
import cn.edu.zju.jsonStreamer.operators.Operator;
import cn.edu.zju.jsonStreamer.operators.ProjectionOperator;
import cn.edu.zju.jsonStreamer.operators.RStreamOperator;
import cn.edu.zju.jsonStreamer.operators.RangeWindowOperator;
import cn.edu.zju.jsonStreamer.operators.RootOperator;
import cn.edu.zju.jsonStreamer.operators.RowWindowOperator;
import cn.edu.zju.jsonStreamer.operators.SelectionOperator;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class APIParserForLocal {
	private JStreamOutput outStream;
	private List<Operator> opList;
	
	public List<Operator> parse(String jsonApiString, JStreamOutput outStream) throws JsonStreamerException{
		List<JsonQueryTree> jqt = new Gson().fromJson(jsonApiString, new TypeToken<List<JsonQueryTree> >(){}.getType());
		this.outStream = outStream;
		opList = new LinkedList<Operator>();
		Iterator<JsonQueryTree> it = jqt.iterator();
		JsonQueryTree curTree;
		while(it.hasNext()){
			curTree = it.next();
			parse(curTree);
		}

		return opList;
	}
	
	private Operator parse(JsonQueryTree tree) throws JsonStreamerException{
		Operator retOp = null, subOp = null, subOp2 = null;
		
		switch (tree.type) {
		case ROOT:
			retOp = new RootOperator(tree, outStream);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case LEAF:
			retOp = new LeafOperator(tree);
			break;
		case ROWWINDOW:
			retOp = new RowWindowOperator(tree);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case RANGEWINDOW:
			retOp = new RangeWindowOperator(tree);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case DSTREAM:
			retOp = new DStreamOperator(tree);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case RSTREAM:
			retOp = new RStreamOperator(tree);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case ISTREAM:
			retOp = new IStreamOperator(tree);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case SELECTION:
			retOp = new SelectionOperator(tree);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case GROUPBY_AGGREGATION:
			retOp = new AggregationOperator(tree);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case EXPAND:
			retOp = new ExpandOperator(tree);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case PROJECTION:
			retOp = new ProjectionOperator(tree);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case JOIN:
			retOp = new JoinOperator(tree);
			subOp = parse(tree.left_input);
			subOp2 = parse(tree.right_input);
			connectOperators(retOp, subOp, subOp2);
			break;
		case PARTITIONWINDOW:
			throw new SemanticErrorException("partition window not supported yet");
		case ERROR:
			JsonError error = tree.error_info;
			switch (error.error_type) {
			case SEMANTIC_ERROR:
				throw new SemanticErrorException(error.error_message);
			case SYNTAX_ERROR:
				throw new SyntaxErrorException(error.error_message);
			}
			break;
			
		default:
			throw new SystemErrorException("shouldn't be here");
		}
		
		opList.add(retOp);
		return retOp;
	}
	
	private void connectOperators(Operator recOp, Operator sendOp){
		Queue<MarkedElement> queue = new LinkedList<MarkedElement>();
		recOp.addInputQueue(queue);
		sendOp.addOutputQueue(queue);
	}
	private void connectOperators(Operator recOp, Operator sendOpLeft, Operator sendOpRight){
		Queue<MarkedElement> queueLeft, queueRight;
		queueLeft = new LinkedList<MarkedElement>();
		queueRight = new LinkedList<MarkedElement>();
		recOp.addInputQueue(queueLeft);
		recOp.addInputQueue(queueRight);
		sendOpLeft.addOutputQueue(queueLeft);
		sendOpRight.addOutputQueue(queueRight);
	}
}
