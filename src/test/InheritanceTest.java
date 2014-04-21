package test;

public class InheritanceTest {

	public class Father{
		public Father(int num){
			System.out.println("father cstor: "+num);
		}
	}
	
	public class Son extends Father{
		public Son(int num){
			super(num);
			System.out.println("son cstor: "+num);
		}
	}
	public void test1(){
		Father f = new Son(100);
	}
	
	public void test2(){
		double dd = 10.01;
		Object oo = (Object)dd;
		System.out.println(oo instanceof Double);
	}
	public static void main(String[] args) {
		new InheritanceTest().test2();

	}

}
