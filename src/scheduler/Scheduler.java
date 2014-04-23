package scheduler;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import constants.SystemErrorException;

import operators.Operator;

public class Scheduler {
	private List<Operator> opList = null;
	private Iterator<Operator> iterator = null;
	private static Scheduler scheduler = null;
	
	public static Scheduler getInstance(){
		if(scheduler == null) scheduler = new Scheduler();
		
		return scheduler;
	}
	
	public boolean isEmpty(){
		synchronized (opList) {
			return opList == null || opList.isEmpty();
		}
	}
	
	public void addOperator(Operator operator){
		synchronized (opList) {
			if(opList == null) opList = new LinkedList<Operator>();
			opList.add(operator);
			iterator = opList.iterator();
		}
	}
	
	public void addOperators(List<Operator> operatorList){
		synchronized (opList) {
			if(opList == null) opList = new LinkedList<Operator>();
			this.opList.addAll(operatorList);
			iterator = opList.iterator();
		}
	}
	
	public Operator getNextOperator() throws SystemErrorException{
		synchronized (opList) {
			if(isEmpty())
				throw new SystemErrorException("operation list empty");
			if(iterator.hasNext()) return iterator.next();
			else{
				iterator = opList.iterator();
				return iterator.next();
			}
		}
	}
}
