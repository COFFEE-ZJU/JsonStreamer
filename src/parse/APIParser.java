package parse;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import json.MarkedElement;
import jsonAPI.JsonQueryTree;

import IO.JStreamOutput;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import constants.Constants.JsonOpType;

import operators.*;

public class APIParser {
	private JStreamOutput outStream;
	
	public Operator parse(String jsonApiString, JStreamOutput outStream){
		List<JsonQueryTree> jqt = new Gson().fromJson(jsonApiString, new TypeToken<List<JsonQueryTree> >(){}.getType());
		if(jqt.size() == 1 && jqt.get(0).type == JsonOpType.root){
			this.outStream = outStream;
			return parse(jqt.get(0));
		}
		else{
			System.err.println("error occours");	//TODO
			return null;
		}
	}
	
	private Operator parse(JsonQueryTree tree){
		Operator retOp = null, subOp = null, subOp2 = null;
		
		switch (tree.type) {
		case root:
			retOp = parse(tree.input);
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
			retOp = new DStreamOperator(tree, outStream);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case rstream:
			retOp = new RStreamOperator(tree, outStream);
			subOp = parse(tree.input);
			connectOperators(retOp, subOp);
			break;
		case istream:
			retOp = new IStreamOperator(tree, outStream);
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

		default:
			//TODO error occurs
			break;
		}
		
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
		sendOpLeft.addInputQueue(queueLeft);
		sendOpRight.addInputQueue(queueRight);
	}
}
