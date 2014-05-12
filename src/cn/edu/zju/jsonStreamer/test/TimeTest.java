package cn.edu.zju.jsonStreamer.test;

public class TimeTest {

	public void test1() throws InterruptedException{
		System.out.println(System.currentTimeMillis());
		Thread.sleep(1000);
		System.out.println(System.currentTimeMillis());
	}
	public static void main(String[] args) throws InterruptedException {
		new TimeTest().test1();

	}

}
