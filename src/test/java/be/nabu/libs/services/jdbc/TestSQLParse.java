/*
* Copyright (C) 2016 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

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
