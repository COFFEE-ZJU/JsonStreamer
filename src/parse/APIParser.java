package parse;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import json.MarkedElement;
import jsonAPI.JsonQueryTree;

import IO.JStreamOutput;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import constants.SystemErrorException;

import operators.*;

public class APIParser {
	private JStreamOutput outStream;
	private List<Operator> opList;
	
	public List<Operator> parse(String jsonApiString, JStreamOutput outStream){
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
	
	private Operator parse(JsonQueryTree tree){
		Operator retOp = null, subOp = null, subOp2 = null;
		
		switch (tree.type) {
		case root:
			retOp = new RootOperator(tree, outStream);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case leaf:
			retOp = new LeafOperator(tree);
			break;
		case rowwindow:
			retOp = new RowWindowOperator(tree);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case rangewindow:
			retOp = new RangeWindowOperator(tree);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case dstream:
			retOp = new DStreamOperator(tree);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case rstream:
			retOp = new RStreamOperator(tree);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case istream:
			retOp = new IStreamOperator(tree);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case selection:
			retOp = new SelectionOperator(tree);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case groupby_aggregation:
			retOp = new AggregationOperator(tree);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case expand:
			retOp = new ExpandOperator(tree);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case projection:
			retOp = new ProjectionOperator(tree);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case join:
			retOp = new JoinOperator(tree);
			subOp = parse(tree.left_input);
			subOp2 = parse(tree.right_input);
			connectOperators(retOp, subOp, subOp2);
			break;
		case partitionwindow:
			//TODO not supported now
			break;
		case error:
			throw new SystemErrorException(tree.error_message);
			
		default:
			//TODO error occurs
			break;
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
