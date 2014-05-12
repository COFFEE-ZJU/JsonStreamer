package cn.edu.zju.jsonStreamer.test;

public class SyncTest extends Thread{
	public String sync = "sync";
	
	public SyncTest(String name){
		sync = name;
	}
	
	public void test2(){
		synchronized (sync) {
			while(true){
				System.out.println("int test2");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public void test(){
		synchronized (sync) {
			while(true){
				System.out.println("int test");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	@Override
	public void run(){
		test();
	}
	
	public static void main(String[] args) {
		SyncTest st = new SyncTest("name1");
		st.start();
		new SyncTest("name1").test2();
	}
}
