package utils;

public abstract class StoppableThread extends Thread{
	private boolean inExecution = true;
	public void stopTheThread(){
		inExecution = false;
	}
	
	protected abstract void initialize() throws Exception;
	protected abstract void inLoop() throws Exception;
	protected abstract void finish() throws Exception;
	protected abstract void inException(Exception e);
	
	@Override
	public final void run(){
		try{
			initialize();
			while(inExecution)
				inLoop();
			
			finish();
		} catch (Exception e) {
			inException(e);
		}
	}
}
