package be.nabu.libs.services.jdbc;

import be.nabu.libs.services.jdbc.api.JDBCTranslator.TranslationBinding;
import be.nabu.libs.types.SimpleTypeWrapperFactory;
import be.nabu.libs.types.api.DefinedSimpleType;
import be.nabu.libs.types.base.SimpleElementImpl;
import be.nabu.libs.types.base.ValueImpl;
import be.nabu.libs.types.properties.PrimaryKeyProperty;
import be.nabu.libs.types.properties.TranslatableProperty;
import be.nabu.libs.types.structure.Structure;
import junit.framework.TestCase;

public class TestTranslationBinding extends TestCase {
	public void testBinding() {
		Structure structure = new Structure();
		DefinedSimpleType<String> wrap = SimpleTypeWrapperFactory.getInstance().getWrapper().wrap(String.class);
		structure.add(new SimpleElementImpl<String>("id", wrap, structure, new ValueImpl<Boolean>(PrimaryKeyProperty.getInstance(), true)));
		structure.add(new SimpleElementImpl<String>("name", wrap, structure));
		structure.add(new SimpleElementImpl<String>("title", wrap, structure, new ValueImpl<Boolean>(TranslatableProperty.getInstance(), true)));
		
		TranslationBinding binding = new TranslationBinding();
		binding.setTranslationTable("translations");
		binding.setFieldName("name");
		binding.setFieldId("instance_id");
		binding.setFieldTranslation("translation");
		binding.setFieldLanguage("language_id");
		
//		String rewriteTranslated = JDBCServiceInstance.rewriteTranslated("select t.id, t.name, t.title from tests t where t.title like '%test%'", structure, binding, "en");
//		System.out.println(rewriteTranslated);
		
		JDBCServiceInstance.rewriteTranslated("select \n" + 
				"	mde.id,\n" + 
				"	mde.created,\n" + 
				"	mde.modified,\n" + 
				"	mde.name,\n" + 
				"	mde.title,\n" + 
				"	mde.description,\n" + 
				"	mde.owner_id,\n" + 
				"	mde.master_data_category_id,\n" + 
				"	mde.priority,\n" + 
				"	mde.operational,\n" + 
				"	mde.disabled,\n" + 
				"	f0_master_data_category_id.name as master_data_category\n" + 
				" from  master_data_entries mde\n" + 
				"	 join master_data_categories f0_master_data_category_id on f0_master_data_category_id.id = mde.master_data_category_id  LIMIT 100", structure, binding, "en");
	}
}
