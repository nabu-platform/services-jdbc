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

import java.util.Arrays;

import junit.framework.TestCase;

public class TestForeignTableParse extends TestCase {
	public void testForeignNameParse1() {
		String foreignName1 = "ownerId:name";
		String foreignName2 = "ownerId:parentId:name";
		String foreignName3 = "ownerId:parentId:siblingId:parentId:ownerId:name";
		assertEquals(
			Arrays.asList("f0_owner_id"),
			JDBCUtils.getForeignNameTables1(foreignName1)
		);
		assertEquals(
			Arrays.asList("f0_owner_id", "f1_owner_id_parent_id"),
			JDBCUtils.getForeignNameTables1(foreignName2)
		);
		assertEquals(
			Arrays.asList("f0_owner_id", "f1_owner_id_parent_id", "f2_owner_parent_siblin", "f3_own_par_sib_par", "f4_ow_pa_si_pa_ow"),
			JDBCUtils.getForeignNameTables1(foreignName3)
		);
	}
	public void testForeignNameParse2() {
		String foreignName1 = "ownerId:name";
		String foreignName2 = "ownerId:parentId:name";
		String foreignName3 = "ownerId:parentId:siblingId:parentId:ownerId:name";
		assertEquals(
			Arrays.asList("f0_o_1989460420"),
			JDBCUtils.getForeignNameTables2(foreignName1)
		);
		assertEquals(
			Arrays.asList("f0_o_1989460420", "f1_op_1389962476"),
			JDBCUtils.getForeignNameTables2(foreignName2)
		);
		assertEquals(
			Arrays.asList("f0_o_1989460420", "f1_op_1389962476", "f2_ops_1612884268", "f3_opsp_1580857470", "f4_opspo_1305684711"),
			JDBCUtils.getForeignNameTables2(foreignName3)
		);
	}
	public void testForeignNameParse() {
		String foreignName1 = "ownerId:name";
		String foreignName2 = "ownerId:parentId:name";
		String foreignName3 = "ownerId:parentId:siblingId:parentId:ownerId:name";
		assertEquals(
			Arrays.asList("f0_oi_1989460420"),
			JDBCUtils.getForeignNameTables(foreignName1)
		);
		assertEquals(
			Arrays.asList("f0_oi_1989460420", "f1_oi_pi_1389962476"),
			JDBCUtils.getForeignNameTables(foreignName2)
		);
		assertEquals(
			Arrays.asList("f0_oi_1989460420", "f1_oi_pi_1389962476", "f2_oi_pi_si_1612884268", "f3_oi_pi_si_pi_1580857470", "f4_oi_pi_si_pi_oi_1305684711"),
			JDBCUtils.getForeignNameTables(foreignName3)
		);
	}
}
