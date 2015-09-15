package cf;

import java.util.ArrayList;

import org.json.simple.JSONObject;

import cf.SimpleRecommendation;

class Test {

	public static void main(String[] args) throws Exception {

//		ArrayList<String> x = new ArrayList<String>();
		SimpleRecommendation sr = new SimpleRecommendation();
//		sr.delete_DB("127.0.0.1", 27017, "ec_revo_analysis_development", "customers", "5253a7e2a893bd6ee5000001", "522957dfa893bd3bc1000001");
//		sr.delete_DB("127.0.0.1", 27017, "ec_revo_analysis_development", "customer_sorts", "5253a7e2a893bd6ee5000001", "522957dfa893bd3bc1000001");
//		sr.delete_DB("127.0.0.1", 27017, "ec_revo_analysis_development", "customer_orders", "5253a7e2a893bd6ee5000001", "522957dfa893bd3bc1000001");
//		sr.delete_DB("127.0.0.1", 27017, "ec_revo_analysis_development", "items", "5253a7e2a893bd6ee5000001", "522957dfa893bd3bc1000001");
//		sr.delete_DB("127.0.0.1", 27017, "ec_revo_analysis_development", "customer_recommended_items", "5253a7e2a893bd6ee5000001", "522957dfa893bd3bc1000001");
//		sr.import_CSV("127.0.0.1", 27017, "ec_revo_analysis_development", "out_customer.csv", "customers", "5253a7e2a893bd6ee5000001", "5253a7e2a893bd6ee5000001");
		sr.import_CSV("127.0.0.1", 27017, "ec_revo_analysis_development", "NSN_item_production.csv", "item_test", "5318424772c286a99f000001", "5311b44972c2862ec2000001");
//		sr.import_customer_portfolio("127.0.0.1", 27017, "ec_revo_analysis_development", "out_order.csv", "customer_portfolio_orders", "customer_orders", "5253a7e2a893bd6ee5000001", "5253a7e2a893bd6ee5000001");
//		sr.import_customer_portfolio("127.0.0.1", 27017, "ec_revo_analysis_development", "orders.csv", "customer_portfolio_orders_test", "customer_orders_test", "5302fe8aa893bd632a000003", "52bb9392a893bdb0bf000001");

//		sr.get_recommended_items("127.0.0.1", 27017, "ec_revo_analysis_development", "customer_cluster_maps", "cluster_recommended_items", 10, "company_customer_id", "company_item_id", "525f82c1a893bdc6b3000001");
//		sr.create_customer_sort_table("127.0.0.1", 27017, "ec_revo_analysis_development", "customers", "customer_recommended_items", "customer_sorts", "5253a7e2a893bd6ee5000001");
//		sr.get_recommended_items("127.0.0.1", 27017, "ec_revo_analysis_development", "customer_orders", "customer_recommended_items", 5, "company_customer_id", "company_item_id", "52735829a893bd9979000004", "525f82c1a893bdc6b3000001");
//		sr.create_customer_sort_table("127.0.0.1", 27017, "ec_revo_analysis_development", "customers", "customer_recommended_items", "customer_sorts", "5253a7e2a893bd6ee5000001", "5253a7e2a893bd6ee5000001");
//		sr.update_csv("127.0.0.1", 27017, "ec_revo_analysis_development", "items.csv", "items", "52a534afa893bdceed000001", "525f82c1a893bdc6b3000001");
		
//		sr.create_customer_order_sort("127.0.0.1", 27017, "ec_revo_analysis_development", "customer_portfolio_orders", "customer_transactions", "52bb947ba893bd5184000007", "52bb9392a893bdb0bf000001");
		
//		ClusterRecommendation cr = new ClusterRecommendation();
//		cr.insert_order("127.0.0.1", 27017, "ec_revo_analysis_development", "customer_orders", "customer_clusters", 400, "company_customer_id", "company_item_id", "525f82c1a893bdc6b3000001", "525f82c1a893bdc6b3000001");
//		MakeShopRecommendation mr = new MakeShopRecommendation();
//		
//		String [] arr_customer = new String[2];
//		arr_customer[0] = "{\"customer_name\":\"Toai\", \"customer_cd\":\"arista\", \"email\":\"toainv@gmail.com\"}";
//		arr_customer[1] = "{\"customer_name\":\"スモール自由雲台 Toai\", \"customer_cd\":\"test\", \"email\":\"toainv@gmail.com\"}";
//		
//		String [] arr_order = new String[2];
//		arr_order[0] = "{\"customer_cd\":\"arista\", \"order_date\":\"2012/02/09 15:05:32\", \"item_cd\":\"182256\"}";
//		arr_order[1] = "{\"customer_cd\":\"arista\", \"order_date\":\"2012/02/09 15:05:32\", \"item_cd\":\"182256\"}";
//		
//		
//		mr.import_customer_and_orders("127.0.0.1", 27017, "ec_revo_analysis_development", arr_customer, arr_order, "customers", "customer_orders", "52735829a893bd9979000004", "525f82c1a893bdc6b3000001");
		
		//byte[] bb = sr.create_Hash_key("abc");
//		String b = "defd";
//		System.out.println(b.hashCode());
//		int abc = sr.get_no_recommended_item("127.0.0.1", 27017, "ec_revo_analysis_development", "customer_recommended_items", "51ff957072c28632f3000001", "8682");
//		System.out.println(abc);
	}

	protected static void test_get_recommended_items_by_user() throws Exception {
		ArrayList<String> x = new ArrayList<String>();
		SimpleRecommendation sr = new SimpleRecommendation();
		x = sr.get_recommended_items_by_user("localhost", 27017,
				"ec_revo_analysis_development", "customer_order",
				"recommendations", "321270", 5);
		System.out.println(x);
	}

	protected static void test_get_recommended_items_by_user_from_file()
			throws Exception {
		ArrayList<String> x = new ArrayList<String>();
		SimpleRecommendation sr = new SimpleRecommendation();
		x = sr.get_recommended_items_by_user_from_file("localhost", 27017,
				"ec_revo_analysis_development", "test.csv", "recommendations",
				1, 5);
		System.out.println(x);
	}
}