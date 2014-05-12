package cn.edu.zju.jsonStreamer.query;

import cn.edu.zju.jsonStreamer.json.Element;
import cn.edu.zju.jsonStreamer.jsonAPI.JsonExpression;

public class JoinEqDealer {
	private ExpressionDealer leftDealer, rightDealer;
	
	public JoinEqDealer(JsonExpression left, JsonExpression right){
		leftDealer = ExpressionDealer.genExpressionDealer(left);
		leftDealer = ExpressionDealer.genExpressionDealer(right);
	}
	
	public boolean deal(Element leftEle, Element rightEle){
		Element left = leftDealer.deal(leftEle);
		Element right = rightDealer.deal(rightEle);
		
		return left.equals(right);
	}
}
