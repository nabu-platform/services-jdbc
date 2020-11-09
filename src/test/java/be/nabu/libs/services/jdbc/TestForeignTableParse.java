package be.nabu.libs.services.jdbc;

import java.util.Arrays;

import junit.framework.TestCase;

public class TestForeignTableParse extends TestCase {
	public void testForeignNameParse() {
		String foreignName1 = "ownerId:name";
		String foreignName2 = "ownerId:parentId:name";
		String foreignName3 = "ownerId:parentId:siblingId:parentId:ownerId:name";
		assertEquals(
			Arrays.asList("f_owner_id"),
			JDBCUtils.getForeignNameTables(foreignName1)
		);
		assertEquals(
			Arrays.asList("f_owner_id", "f_owner_id_parent_id"),
			JDBCUtils.getForeignNameTables(foreignName2)
		);
		assertEquals(
			Arrays.asList("f_owner_id", "f_owner_id_parent_id", "f_owner_parent_siblin", "f_own_par_sib_par", "f_ow_pa_si_pa_ow"),
			JDBCUtils.getForeignNameTables(foreignName3)
		);
	}
}
