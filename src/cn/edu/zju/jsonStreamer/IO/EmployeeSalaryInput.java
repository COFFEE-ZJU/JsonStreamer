package cn.edu.zju.jsonStreamer.IO;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import com.google.gson.Gson;

public class EmployeeSalaryInput extends FileStreamInput{

	public EmployeeSalaryInput() {
		super("inputFiles/employeeSalary.txt");
	}

	public static class Info{
		public String dept;
		public String name;
		public int salary;
		public boolean is_manager;
		public Info(String dept, String name, int salary, boolean is_manager){
			this.dept = dept;
			this.name = name;
			this.salary = salary;
			this.is_manager = is_manager;
		}
	}
	
	public static void main(String[] args) throws IOException {
		
		File file = new File("inputFiles/employeeSalary.txt");
		BufferedWriter bw = new BufferedWriter(new FileWriter(file));
		int i;
		String dept = "IT";
		String name = "it_name";
		int baseSal = 10000;
		Random ran = new Random(1024);
		Gson gson = new Gson();
		for(i=1;i<=20;i++){
			bw.append(gson.toJson(
					new Info(dept, name+i, ran.nextInt(10000)+baseSal, ran.nextBoolean())));
			System.out.println(gson.toJson(
					new Info(dept, name+i, ran.nextInt(10000)+baseSal, ran.nextBoolean())));
			bw.newLine();
		}
		
		dept = "HR";
		name = "hr_name";
		baseSal = 6000;
		for(i=1;i<=10;i++){
			bw.append(gson.toJson(
					new Info(dept, name+i, ran.nextInt(5000)+baseSal, ran.nextBoolean())));
			bw.newLine();
		}
		
		dept = "managing";
		name = "manage_name";
		baseSal = 9000;
		for(i=1;i<=23;i++){
			bw.append(gson.toJson(
					new Info(dept, name+i, ran.nextInt(5000)+baseSal, ran.nextBoolean())));
			bw.newLine();
		}
		
		bw.flush();
		bw.close();
	}
}
