package parse;

import java.util.List;

import jsonAPI.JsonQueryTree;

import IO.IOManager;
import IO.JStreamInput;
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
		Operator retOp = null, subOp = null;
		
		switch (tree.type) {
		case root:
			retOp = new RootOperator(outStream);
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
			

		default:
			break;
		}
		
		return retOp;
	}
	
	private void connectOperators(Operator recOp, Operator sendOp){
		//TODO
	}
}
