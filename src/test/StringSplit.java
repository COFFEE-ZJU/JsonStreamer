package test;

public class StringSplit {
	public static void main(String[] args) {
		String[] s = "an d time".split(" ");
		System.out.println(s.length);
		
		System.out.println("and, or, not, gt, ge, lt, le, eq, ne, bool".toUpperCase());
		
		System.out.println("abf".compareTo("abde"));
	}
}
