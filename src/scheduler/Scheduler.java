package scheduler;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import constants.SystemErrorException;

import operators.Operator;
import utils.StoppableThread;

public class Scheduler extends StoppableThread{
	private List<Operator> opList = null;
	private Iterator<Operator> iterator = null;
	private Operator currentOp;
	private static Scheduler scheduler = null;
	
	private Scheduler(){
		opList = new LinkedList<Operator>();
	}
	
	public static Scheduler getInstance(){
		if(scheduler == null) scheduler = new Scheduler();
		
		return scheduler;
	}
	
	@Override
	protected void initialize() throws Exception {
		System.out.println("Scheduler starts serving");
	}

	@Override
	protected void inLoop() throws Exception {
		if(isEmpty()){
			try {
				System.out.println("scheduler sleeping...");
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return;
		}
		
		try {
			currentOp = getNextOperator();
			currentOp.execute();
		} catch (SystemErrorException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void finish() throws Exception {
		System.out.println("Scheduler stops serving");
	}

	@Override
	protected void inException(Exception e) {
		e.printStackTrace();
	}
	
//	@Override
//	public void run() {
//		Scheduler scheduler = Scheduler.getInstance();
//		Operator operator;
//		while(true){
//			if(scheduler.isEmpty()){
//				try {
//					Thread.sleep(100);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//				continue;
//			}
//			
//			try {
//				operator = scheduler.getNextOperator();
//				operator.execute();
//			} catch (SystemErrorException e) {
//				e.printStackTrace();
//			}
//			
//		}
//	}
	
	public boolean isEmpty(){
		synchronized (opList) {
			return opList.isEmpty();
		}
	}
	
	public void addOperator(Operator operator){
		synchronized (opList) {
			opList.add(operator);
			iterator = opList.iterator();
		}
	}
	
	public void addOperators(List<Operator> operatorList){
		synchronized (opList) {
			this.opList.addAll(operatorList);
			iterator = opList.iterator();
		}
	}
	
	public boolean removeOperator(Operator operator){
		synchronized (opList) {
			if(! opList.remove(operator))
				return false;
			
			iterator = opList.iterator();
			return true;
		}
	}
	
	public boolean removeOperators(List<Operator> operatorList){
		synchronized (opList) {
			if(! opList.removeAll(operatorList))
				return false;
			
			iterator = opList.iterator();
			return true;
		}
	}
	
	private Operator getNextOperator() throws SystemErrorException{
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
