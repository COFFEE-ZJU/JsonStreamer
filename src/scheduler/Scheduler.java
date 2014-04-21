package scheduler;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import constants.SystemErrorException;

import operators.Operator;

public class Scheduler {
	private List<Operator> opList = null;
	private Iterator<Operator> iterator = null;
	
	public Scheduler(List<Operator> opList){
		this.opList = new LinkedList<Operator>(opList);
		iterator = this.opList.iterator();
	}
	
	public boolean isEmpty(){
		return opList == null || opList.isEmpty();
	}
	
	public void addOperator(Operator operator){
		opList.add(operator);
		iterator = opList.iterator();
	}
	
	public void addOperators(List<Operator> operatorList){
		this.opList.addAll(operatorList);
		iterator = opList.iterator();
	}
	
	public Operator getNextOperator(){
		if(isEmpty())
			throw new SystemErrorException("operation list empty");
		if(iterator.hasNext()) return iterator.next();
		else{
			iterator = opList.iterator();
			return iterator.next();
		}
	}
}
