package be.nabu.libs.services.jdbc;

import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.Element;

public class TestSQLParse {
	public static void main(String...args) {
		String sql = "select a, b, sum(c) as d from mytable where test = :test and jimbob in (:list)";
		JDBCService service = new JDBCService("test");
		service.setSql(sql);
		print(service);
		service.setSql("insert into mytable (field1, field2) values (:field1, :field2)");
		print(service);
	}

	private static void print(JDBCService service) {
		for (Element<?> child : (ComplexType) service.getInput().get(JDBCService.PARAMETERS).getType()) {
			System.out.println("Input: " + child.getName());
		}
		
		for (Element<?> child : (ComplexType) service.getOutput().get(JDBCService.RESULTS).getType()) {
			System.out.println("Output: " + child.getName());
		}
	}
}
