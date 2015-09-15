package cf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericBooleanPrefUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.bson.types.ObjectId;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MakeShopRecommendation {
	protected void import_items(String DB_host, int DB_port, String DB_name,
			String file_input, String DB_Out_tbl, String shop_id,
			String company_id) throws Exception {
		try {
			@SuppressWarnings("deprecation")
			Mongo mongo = new Mongo(DB_host, DB_port); // 27017

			DB db = mongo.getDB(DB_name);

			DBCollection collection = db.getCollection(DB_Out_tbl);

			BasicDBObject query = new BasicDBObject();
			ObjectId company_obj_id = new ObjectId(company_id);
			ObjectId shop_obj_id = new ObjectId(shop_id);
			query.put("company_id", company_obj_id);
			query.put("shop_id", shop_obj_id);
			collection.remove(query);

			File fleExample = new File(file_input);
			// Find out if the file exists already
			if (fleExample.exists()) {
				BufferedReader in = new BufferedReader(new InputStreamReader(
						new FileInputStream(fleExample), "UTF-8"));
				String str = in.readLine();

				String[] header = new String[] { "company_item_id",
						"company_item_cd", "item_name", "item_price",
						"description", "out_of_order", "click_link" };
				int index = 1;
				
				BasicDBObject document = new BasicDBObject();
				while ((str = in.readLine()) != null) {
					document.clear();
					// Read each line and display its value
					String[] parts = str.split(",");
					document.put(header[0], String.valueOf(index));
					document.put(header[1], parts[2]);
					document.put(header[2], parts[6]);
					document.put(header[3], parts[8]);
					document.put(header[4], parts[49]);
					document.put(header[5], parts[51]);
					document.put(header[6], parts[69]);

					document.put("company_id", company_obj_id);
					document.put("shop_id", shop_obj_id);

					collection.save(document);
					
					index ++;
				}
				System.out.println("Done");
				in.close();
			} else
				// if( !fleExample.exists() )
				System.out.println("No file exists with that name");

		} catch (NoSuchElementException e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (UnknownHostException e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}
	
	// Customer buying
	protected void import_customer_buying(String DB_host, int DB_port, String DB_name,
		    String[] order_arr, String DB_Out_tbl, String shop_id,
			String company_id) throws Exception {
		try {
			@SuppressWarnings("deprecation")
			Mongo mongo = new Mongo(DB_host, DB_port); // 27017

			DB db = mongo.getDB(DB_name);

			DBCollection order_collection = db.getCollection(DB_Out_tbl);
			DBCollection customer_collection = db.getCollection("customers");
			DBCollection item_collection = db.getCollection("items");

			BasicDBObject query = new BasicDBObject();
			ObjectId company_obj_id = new ObjectId(company_id);
			ObjectId shop_obj_id = new ObjectId(shop_id);
			query.put("company_id", company_obj_id);
			query.put("shop_id", shop_obj_id);
			order_collection.remove(query);
			
			for (int i = 0; i < order_arr.length; i++) {
				JSONObject json = (JSONObject) new JSONParser()
						.parse(order_arr[i].toString());
				String order_no = (String) json.get("order_no");
				String customer_cd = (String) json.get("customer_cd");
				String item_cd = (String) json.get("item_cd");
				String order_date = (String) json.get("order_date");
				String amount = (String) json.get("amount");
				// Find company_customer_id
				BasicDBObject find_customer_id = new BasicDBObject();
				find_customer_id.put("company_customer_cd", customer_cd);
				find_customer_id.put("company_id", company_obj_id);
				find_customer_id.put("shop_id", shop_obj_id);

				// Find company_item_id
				BasicDBObject find_item_id = new BasicDBObject();
				find_item_id.put("company_item_cd", item_cd);
				find_item_id.put("company_id", company_obj_id);
				find_item_id.put("shop_id", shop_obj_id);

				DBCursor cursor_customer = customer_collection.find(find_customer_id);
				DBCursor cursor_item = item_collection.find(find_item_id);
				
				if (cursor_customer.hasNext() && cursor_item.hasNext()) {
					// Get company_customer_id
					DBObject customer_row = cursor_customer.next();
					Object company_customer_id = (Object) customer_row.get("company_customer_id");

					// Get company_item_id
					DBObject item_row = cursor_item.next();
					Object company_item_id = (Object) item_row.get("company_item_id");

					// Save customer order to db
					BasicDBObject document = new BasicDBObject();
					document.put("order_no", order_no);
					document.put("company_customer_cd", customer_cd);
					document.put("company_item_cd", item_cd);
					document.put("company_customer_id", company_customer_id.toString());
					document.put("company_item_id", company_item_id.toString());
					document.put("amount", amount);

					Date tempDate = new SimpleDateFormat("yyyy/MM/dd").parse(order_date);
					document.put("order_date", tempDate);
					document.put("company_id", company_obj_id);
					document.put("shop_id", shop_obj_id);
					
					order_collection.save(document);
					System.out.println("Insert Customer Buying Order " + document);
				}
			}
		} catch (NoSuchElementException e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (UnknownHostException e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}
	
	// Import customer and order
	protected void import_customer_and_orders(String DB_host, int DB_port,
			String DB_name, String[] customer_arr, String[] order_arr,
			String DB_Out_customer, String DB_Out_portfolio_order, String DB_Out_order, String shop_id,
			String company_id) throws Exception {
		try {
			@SuppressWarnings("deprecation")
			Mongo mongo = new Mongo(DB_host, DB_port); // 27017

			DB db = mongo.getDB(DB_name);
			DBCollection customer_collection = db
					.getCollection(DB_Out_customer);
			DBCollection order_collection = db.getCollection(DB_Out_order);
			DBCollection portfolio_order_collection = db.getCollection(DB_Out_portfolio_order);
			DBCollection item_collection = db.getCollection("items");
			BasicDBObject query = new BasicDBObject();
			ObjectId company_obj_id = new ObjectId(company_id);
			ObjectId shop_obj_id = new ObjectId(shop_id);
			query.put("company_id", company_obj_id);
			query.put("shop_id", shop_obj_id);

			// Remove old data
			customer_collection.remove(query);
			order_collection.remove(query);
			portfolio_order_collection.remove(query);

			// Insert customer data
			int customer_index = 1;
			for (int i = 0; i < customer_arr.length; i++) {
				JSONObject json = (JSONObject) new JSONParser()
						.parse(customer_arr[i].toString());
				String name = (String) json.get("customer_name");
				String email = (String) json.get("email");
				String customer_cd = (String) json.get("customer_cd");
				// Find customer
				BasicDBObject find_query = new BasicDBObject();
				find_query.put("customer_cd", customer_cd);
				find_query.put("company_id", company_obj_id);
				find_query.put("shop_id", shop_obj_id);
				DBCursor data = customer_collection.find(find_query);

				if (!data.hasNext()) {
					// Customer does not exist, insert new
					BasicDBObject document = new BasicDBObject();
					document.put("company_customer_id", String.valueOf(customer_index));
					document.put("company_customer_cd", customer_cd);
					document.put("customer_name", name);
					document.put("email", email);
					document.put("delivered", 0);
					document.put("receive_status", 1);
					document.put("company_id", company_obj_id);
					document.put("shop_id", shop_obj_id);
					
					customer_collection.save(document);
					System.out.println("Insert Customer" + document);
					customer_index++;
				}
			}

			// Insert customer order, customer portfolio order data
			for (int i = 0; i < order_arr.length; i++) {
				JSONObject json = (JSONObject) new JSONParser()
						.parse(order_arr[i].toString());
				String order_no = (String) json.get("order_no");
				String customer_cd = (String) json.get("customer_cd");
				String item_cd = (String) json.get("item_cd");
				String order_date = (String) json.get("order_date");
				String status_order = (String) json.get("status_order");
				String amount = (String) json.get("amount");
				// Find company_customer_id
				BasicDBObject find_customer_id = new BasicDBObject();
				find_customer_id.put("company_customer_cd", customer_cd);
				find_customer_id.put("company_id", company_obj_id);
				find_customer_id.put("shop_id", shop_obj_id);

				// Find company_item_id
				BasicDBObject find_item_id = new BasicDBObject();
				find_item_id.put("company_item_cd", item_cd);
				find_item_id.put("company_id", company_obj_id);
				find_item_id.put("shop_id", shop_obj_id);

				DBCursor cursor_customer = customer_collection
						.find(find_customer_id);
				DBCursor cursor_item = item_collection.find(find_item_id);
				if (cursor_customer.hasNext() && cursor_item.hasNext()) {
					// Find company_customer_id
					DBObject customer_row = cursor_customer.next();
					Object company_customer_id = (Object) customer_row
							.get("company_customer_id");

					// Find company_item_id
					DBObject item_row = cursor_item.next();
					Object company_item_id = (Object) item_row
							.get("company_item_id");

					// Save customer order to db
					BasicDBObject document = new BasicDBObject();
					document.put("order_no", order_no);
					document.put("company_customer_id",
							company_customer_id.toString());
					document.put("company_customer_cd", customer_cd);
					document.put("amount", Integer.parseInt(amount));
					document.put("company_item_id", company_item_id.toString());
					document.put("status_order", status_order);	
					Date tempDate = new SimpleDateFormat("yyyy/MM/dd").parse(order_date);
//					document.put("order_date", tempDate);
					document.put("company_id", company_obj_id);
					document.put("shop_id", shop_obj_id);
					
					order_collection.save(document);
					System.out.println("Insert Customer Order " + document);
					
					// Save customer portfolio order to db
					BasicDBObject portfolio_document = new BasicDBObject();
					portfolio_document.put("order_no", order_no);
					portfolio_document.put("company_customer_id",
							company_customer_id.toString());
					portfolio_document.put("company_item_id", company_item_id.toString());
					portfolio_document.put("company_item_cd", item_cd);
					portfolio_document.put("company_customer_cd", customer_cd);
					portfolio_document.put("order_date", tempDate);
					portfolio_document.put("amount", Integer.parseInt(amount));
					portfolio_document.put("status_order", status_order);
					portfolio_document.put("company_id", company_obj_id);
					portfolio_document.put("shop_id", shop_obj_id);
					portfolio_order_collection.save(portfolio_document);
					
					System.out.println("Insert Customer Portfolio Order " + portfolio_document);
				}
			}

		} catch (NoSuchElementException e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}
}
