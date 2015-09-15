package cf;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.neighborhood.*;
import org.apache.mahout.cf.taste.impl.recommender.*;
import org.apache.mahout.cf.taste.impl.similarity.*;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.*;
import org.apache.mahout.cf.taste.recommender.*;
import org.apache.mahout.cf.taste.similarity.*;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.model.mongodb.MongoDBDataModel;
import org.bson.types.ObjectId;
import org.mortbay.util.ajax.JSON;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.*;

import cf.MongoDBDataModelExtended;

public class SimpleRecommendation {

	public SimpleRecommendation() {
	}

	public static void main(String[] args) throws Exception {
	}

	protected byte[] create_Hash_key(String word) {
		// ArrayList<String> list = new ArrayList<String> ();
		byte[] bb = null;
		try {
			MessageDigest md;
			md = MessageDigest.getInstance("MD5");

			// List<byte[]> hashes = new ArrayList<byte[]>(list.size());
			// hashes.add(md.digest(word.getBytes("UTF-16LE")));
			bb = md.digest(word.getBytes("UTF-16LE"));
		} catch (NoSuchAlgorithmException e1) {
			e1.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		System.out.println(bb);
		return bb;
	}

	protected int get_no_recommended_item(String DB_host, int DB_port,
			String DB_name, String DB_table, String company_id,
			String company_customer_id) throws Exception {
		try {
			Mongo mongo = new Mongo(DB_host, DB_port);

			DB db = mongo.getDB(DB_name);

			DBCollection collection = db.getCollection(DB_table);
			BasicDBObject query = new BasicDBObject();

			ObjectId company_obj_id = new ObjectId(company_id);
			query.put("company_id", company_obj_id);
			// ObjectId company_customer_obj_id= new
			// ObjectId(company_customer_id);
			query.put("company_customer_id", company_customer_id);
			DBCursor cursor = collection.find(query);
			// System.out.println("asdsf");
			while (cursor.hasNext()) {
				BasicDBObject result = (BasicDBObject) cursor.next();
				BasicDBList features = (BasicDBList) result
						.get("recommended_items");
				if (features == null) {
					return 0;
				} else {
					return features.size();
				}
			}
		} catch (UnknownHostException e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
		return 0;
	}

	protected void delete_DB(String DB_host, int DB_port, String DB_name,
			String DB_Out_tbl, String shop_id, String company_id) throws Exception {
		try {
			Mongo mongo = new Mongo(DB_host, DB_port); // 27017

			// get database from MongoDB,
			// if database doesn't exists, mongoDB will create it automatically
			DB db = mongo.getDB(DB_name);

			// Get collection from MongoDB, database named "yourDB"
			// if collection doesn't exists, mongoDB will create it
			// automatically
			DBCollection collection = db.getCollection(DB_Out_tbl);
			BasicDBObject query = new BasicDBObject();
			ObjectId company_obj_id = new ObjectId(company_id);
			ObjectId shop_obj_id = new ObjectId(shop_id);
			query.put("company_id", company_obj_id);
			query.put("shop_id", shop_obj_id);
			collection.remove(query);
		} catch (UnknownHostException e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}
	
	// DB_Out_tbl1 = "customer_portfolio_orders"
	// DB_Out_tbl2 = "customer_orders"
	protected void import_customer_portfolio(String DB_host, int DB_port, String DB_name,
			String file_input, String DB_Out_tbl1, String DB_Out_tbl2, String shop_id ,String company_id)
			throws Exception {
		try {
			@SuppressWarnings("deprecation")
			Mongo mongo = new Mongo(DB_host, DB_port); // 27017

			DB db = mongo.getDB(DB_name);

			// Get collection from MongoDB, database named "yourDB"
			// if collection doesn't exists, mongoDB will create it
			// automatically
			DBCollection collection1 = db.getCollection(DB_Out_tbl1); // Customer portfolio orders
			DBCollection collection2 = db.getCollection(DB_Out_tbl2); // Customer orders
			DBCollection collection_customer = db.getCollection("customers");
			DBCollection collection_item = db.getCollection("items");
			
			BasicDBObject query = new BasicDBObject();
			ObjectId company_obj_id = new ObjectId(company_id);
			ObjectId shop_obj_id = new ObjectId(shop_id);
			query.put("company_id", company_obj_id);
			query.put("shop_id", shop_obj_id);
			collection1.remove(query);
			collection2.remove(query);

			File fleExample = new File(file_input);
			// Find out if the file exists already
			if (fleExample.exists()) {
				BufferedReader in = new BufferedReader(new InputStreamReader(
						new FileInputStream(fleExample), "UTF-8"));
				String str = in.readLine();

				String[] header_parts = str.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

				BasicDBObject document1 = new BasicDBObject();
				BasicDBObject document2 = new BasicDBObject();
				while ((str = in.readLine()) != null) {
					document1.clear();
					document2.clear();
					// Read each line and display its value
					String[] parts = str.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); // order_no, company_customer_cd, company_item_cd, amount, order_date
					
					// Find company_customer_id
					BasicDBObject customer_find = new BasicDBObject();
					BasicDBObject item_find = new BasicDBObject();
					customer_find.put("company_id", company_obj_id);
					customer_find.put("shop_id", shop_obj_id);
					customer_find.put("company_customer_cd", parts[1]);
					
					// Find company_item_id
					item_find.put("company_id", company_obj_id);
					item_find.put("shop_id", shop_obj_id);
					item_find.put("company_item_cd", parts[2]);
					DBCursor cur_customer = collection_customer.find(customer_find);
					DBCursor cur_item = collection_item.find(item_find);
					
					if (cur_customer.hasNext() && cur_item.hasNext())
					{
						// Find company_customer_id
						DBObject customer_row = cur_customer.next();
						Object company_customer_id = (Object) customer_row.get("company_customer_id");

						// Find company_item_id
						DBObject item_row = cur_item.next();
						Object company_item_id = (Object) item_row.get("company_item_id");
						
						// Import customer portfolio orders
						document1.put("company_customer_id", company_customer_id.toString());
						document1.put("company_item_id", company_item_id.toString());
						document1.put(header_parts[0], parts[0]); // Order no
						document1.put(header_parts[1], parts[1]); // Company customer cd
						document1.put(header_parts[2], parts[2]); // Company item cd
						document1.put(header_parts[3], Integer.parseInt(parts[3])); // Amount
						Date tempDate = new SimpleDateFormat("yyyy/MM/dd").parse(parts[4]);
						document1.put(header_parts[4], tempDate); // Order date
						document1.put(header_parts[5], parts[5]); // Status order
						document1.put("company_id", company_obj_id);
						document1.put("shop_id", shop_obj_id);
						collection1.save(document1);
						
						// Import Customer Orders
						document2.put("company_customer_id", company_customer_id.toString());
						document2.put("company_item_id", company_item_id.toString());
						document2.put(header_parts[0], parts[0]); // Order no
						document2.put(header_parts[1], parts[1]); // Company customer_cd
						document2.put(header_parts[2], parts[2]); // Company item cd
						document2.put(header_parts[3], parts[3]); // Amount
						document2.put(header_parts[5], parts[5]); // Status order
						document2.put("company_id", company_obj_id);
						document2.put("shop_id", shop_obj_id);
						collection2.save(document2);
					}
				}
				System.out.println("Done");
				// De-allocate the memory that was used by the scanner
				// opnScanner.close();
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
	
	// Create customer order sort
	protected void create_customer_order_sort(String DB_host, int DB_port, String DB_name, String DB_In_tbl,
			String DB_Out_tbl, String shop_id, String company_id) throws Exception{
		try{
			@SuppressWarnings("deprecation")
			Mongo mongo = new Mongo(DB_host, DB_port); // 27017
			DB db = mongo.getDB(DB_name);
			
			DBCollection collection_in = db.getCollection(DB_In_tbl);
			DBCollection collection_out = db.getCollection(DB_Out_tbl);
			DBCollection rec_collection = db.getCollection("customer_recommended_items");
			DBCollection customer_sort_collection = db.getCollection("customer_sorts");
			
			BasicDBObject query = new BasicDBObject();
			ObjectId company_obj_id = new ObjectId(company_id);
			ObjectId shop_obj_id = new ObjectId(shop_id);
			query.put("company_id", company_obj_id);
			query.put("shop_id", shop_obj_id);
			// Remove old customer order sort
			collection_out.remove(query);
			
			// Grouping object base on company_id, shop_id
			DBObject match = new BasicDBObject("$match", new BasicDBObject("company_id", company_obj_id).append("shop_id", shop_obj_id));
			
			// Create new customer order sort
			DBObject groupFields = new BasicDBObject( "_id", new BasicDBObject("order_no", "$order_no")); 
			DBObject group = new BasicDBObject("$group", groupFields );

		    DBObject sortFields = new BasicDBObject("order_no", -1);
		    DBObject sort = new BasicDBObject("$sort", sortFields );
		    
		    AggregationOutput output = collection_in.aggregate(match, group, sort);
//		    System.out.print(output.getCommandResult());
		    @SuppressWarnings("rawtypes")
			Iterator it = output.results().iterator();
		    // Looping result to get unique order no
		    while(it.hasNext())
		    {
		    	// Get order no
		    	BasicDBObject _id = (BasicDBObject)it.next();
		    	BasicDBObject order_no_obj = (BasicDBObject)_id.get("_id");
		    	String order_no = order_no_obj.getString("order_no");
		    	BasicDBList item_list = new BasicDBList();
		    	int no_of_items = 0;
		    	int delivered = 0;
		    	
		    	Date order_date = new Date();
		    	String status_order = new String();
		    	String company_customer_id = new String();
		    	String company_customer_cd = new String();
		    	
		    	// Query customer order with order_no found
		    	BasicDBObject findQuery = new BasicDBObject();
		    	findQuery.put("company_id", company_obj_id);
		    	findQuery.put("shop_id", shop_obj_id);
		    	findQuery.put("order_no", order_no);
		    	DBCursor cursor = collection_in.find(findQuery);
		    	
		    	// Get 1st row
		    	if (cursor.hasNext())
		    	{
		    		DBObject firstRow = cursor.next();
		    		order_date = (Date) firstRow.get("order_date");
			    	status_order = (String) firstRow.get("status_order");
			    	company_customer_id = (String) firstRow.get("company_customer_id");
			    	company_customer_cd = (String) firstRow.get("company_customer_cd");
			    	
			    	// Get recommended items
			    	BasicDBObject rec_item_find = new BasicDBObject();
			    	rec_item_find.put("company_id", company_obj_id);
			    	rec_item_find.put("shop_id", shop_obj_id);
			    	rec_item_find.put("company_customer_id", company_customer_id);
			    	DBCursor cur_rec_item = rec_collection.find(rec_item_find);
			    	if (cur_rec_item.hasNext())
			    	{
			    		DBObject cur_row = cur_rec_item.next();
			    		item_list = (BasicDBList) cur_row.get("recommended_items");
//			    		for (int i = 0; i < item_list.size(); i++)
//			    		{
//			    			System.out.println(item_list.toArray()[i]);
//			    		}
			    		no_of_items = item_list.size();
			    	}
			    	
			    	// Get delivery status
			    	BasicDBObject customer_sort = new BasicDBObject();
			    	customer_sort.put("company_id", company_obj_id);
			    	customer_sort.put("shop_id", shop_obj_id);
			    	customer_sort.put("company_customer_id", company_customer_id);
			    	DBCursor cur_customer_sort = customer_sort_collection.find(customer_sort);
			    	if (cur_customer_sort.hasNext())
			    	{
			    		DBObject customer_row = cur_customer_sort.next();
			    		delivered = (Integer)customer_row.get("delivered");
			    	}
		    	}
		    	
		    	// Get status order and order date
		    	while(cursor.hasNext())
		    	{
		    		DBObject row = cursor.next();
					Date order_date_temp = (Date) row.get("order_date");
					if (order_date_temp.getTime() > order_date.getTime())
					{
						// Set new status
						status_order = (String) row.get("status_order");
						order_date = order_date_temp;
					}
		    	}
		    	
		    	BasicDBObject document = new BasicDBObject();
		    	document.put("order_no", order_no);
		    	document.put("status_order", status_order);
		    	document.put("company_customer_id", company_customer_id);
		    	document.put("company_customer_cd", company_customer_cd);
		    	document.put("delivered", delivered);
		    	document.put("recommended_items", item_list);
		    	document.put("no_of_items", no_of_items);
		    	document.put("company_id", company_obj_id);
		    	document.put("shop_id", shop_obj_id);
		    	collection_out.save(document);
		    }
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}
	
	// Import customer or item
	protected void import_CSV(String DB_host, int DB_port, String DB_name,
			String file_input, String DB_Out_tbl, String shop_id, String company_id)
			throws Exception {
		try {
			@SuppressWarnings("deprecation")
			Mongo mongo = new Mongo(DB_host, DB_port); // 27017

			// get database from MongoDB,
			// if database doesn't exists, mongoDB will create it automatically
			DB db = mongo.getDB(DB_name);

			// Get collection from MongoDB, database named "yourDB"
			// if collection doesn't exists, mongoDB will create it
			// automatically
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
				// Prepare a Scanner that will "scan" the document
				// Scanner opnScanner = new Scanner(fleExample);
				// Read each line in the file
				String str = in.readLine();

				String[] header_parts = str.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				int index = 1;
				
				BasicDBObject document = new BasicDBObject();
				while ((str = in.readLine()) != null) {
					document.clear();
					System.out.println(index);
					System.out.println(str);
					// Add id
					if (DB_Out_tbl.equalsIgnoreCase("customers"))
					{
						document.put("company_customer_id", String.valueOf(index++));
						document.put("delivered", 0);
					}
					else if (DB_Out_tbl.equalsIgnoreCase("items"))
					{
						document.put("company_item_id", String.valueOf(index++));
					}
					
					// Read each line and display its value
					String[] parts = str.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
					for (int i = 0; i < parts.length; i++) {
						if (DB_Out_tbl.equalsIgnoreCase("customers") && i == parts.length - 1)
						{
							int opt_in = Integer.parseInt(parts[i]);
							document.put("receive_status", opt_in);
						}
						else
						{
							document.put(header_parts[i], parts[i]);
						}
					}
					document.put("company_id", company_obj_id);
					document.put("shop_id", shop_obj_id);
					
					collection.save(document);
				}
				System.out.println("Done");
				// De-allocate the memory that was used by the scanner
				// opnScanner.close();
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
			String file_input, String DB_Out_tbl, String shop_id, String company_id)
			throws Exception {
		try
		{
			@SuppressWarnings("deprecation")
			Mongo mongo = new Mongo(DB_host, DB_port); // 27017

			DB db = mongo.getDB(DB_name);
			
			// Customer buying collection
			DBCollection collection = db.getCollection(DB_Out_tbl);
			// Customer collection
			DBCollection collection_customer = db.getCollection("customers");

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

				String[] header = new String[] { "company_customer_id",
						"company_item_id", "amount", "order_date" };
				
				BasicDBObject document = new BasicDBObject();
				while ((str = in.readLine()) != null) {
					document.clear();
					// Read each line and display its value
					String[] parts = str.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
					String company_customer_id = parts[0];
					
					// Find company customer id in Customer table
					BasicDBObject customer_find = new BasicDBObject();
					customer_find.put("company_customer_id", company_customer_id);
					customer_find.put("company_id", company_obj_id);
					customer_find.put("shop_id", shop_obj_id);
					DBCursor data = collection_customer.find(customer_find);
					if (data.hasNext())
					{
						document.put(header[0], parts[0]);
						document.put(header[1], parts[1]);
						document.put(header[2], parts[2]);
						
						Date tempDate = new SimpleDateFormat("yyyy/MM/dd").parse(parts[3]);
						document.put("order_date", tempDate);
						document.put("company_id", company_obj_id);
						document.put("shop_id", shop_obj_id);

						collection.save(document);
						System.out.println("Insert Order: " + document);
					}
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
	
	protected void update_csv(String DB_host, int DB_port, String DB_name,
			String file_input, String DB_Out_tbl, String shop_id, String company_id)
			throws Exception {
		try
		{
			@SuppressWarnings("deprecation")
			Mongo mongo = new Mongo(DB_host, DB_port); // 27017
			DB db = mongo.getDB(DB_name);
			
			DBCollection collection = db.getCollection(DB_Out_tbl);
			DBCollection portfolio_order_collection = db.getCollection("customer_portfolio_orders");
			
			ObjectId company_obj_id = new ObjectId(company_id);
			ObjectId shop_obj_id = new ObjectId(shop_id);
			
			File csvFile = new File(file_input);
			// Find out if the file exists already
			if (csvFile.exists()) {
				in = new BufferedReader(new InputStreamReader(
						new FileInputStream(csvFile), "UTF-8"));
				
				// Read header line
				String str = in.readLine();
				String[] header_parts = str.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				
				while ((str = in.readLine()) != null) {
					BasicDBObject searchQuery = new BasicDBObject();
					searchQuery.put("company_id", company_obj_id);
					searchQuery.put("shop_id", shop_obj_id);
					
					// Read each line and display its value
					String[] parts = str.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
					
					String cd_value = "";
					// If customer order => parts[1] is company_customer_cd
					if (DB_Out_tbl.equalsIgnoreCase("customer_orders"))
					{
						cd_value = parts[1];
						searchQuery.put(header_parts[1], cd_value);
					}
					else
					{
						cd_value = parts[0];
						searchQuery.put(header_parts[0], cd_value);
					}
					
					DBCursor data = collection.find(searchQuery);
					if (data.hasNext())
					{
						// Update current row
						BasicDBObject update_query = new BasicDBObject();
						
						update_query.put("company_id", company_obj_id);
						update_query.put("shop_id", shop_obj_id);
						if (DB_Out_tbl.equalsIgnoreCase("customers")) {
							update_query.put("delivered", 0);
							update_query.put("receive_status", 1);
						}
						
						// Put order date if collection is customer_portfolio_orders
						if (DB_Out_tbl.equalsIgnoreCase("customer_portfolio_orders"))
						{
							for (int i = 0; i < 4; i++) {
								update_query.put(header_parts[i], parts[i]);
							}
							collection.update(searchQuery, update_query);
							
							Date tempDate = new SimpleDateFormat("yyyy/MM/dd").parse(parts[4]);
							update_query.put("order_date", tempDate);
							update_query.put("status_order", parts[5]);
							portfolio_order_collection.update(searchQuery, update_query);
							System.out.println("Update with cd: " + parts[1]);
						}
						else
						{
							for (int i = 0; i < parts.length; i++) {
								update_query.put(header_parts[i], parts[i]);
							}
							System.out.println("Update with cd: " + parts[0]);
							collection.update(searchQuery, update_query);
						}
					}
					else // Create new row
					{
						BasicDBObject new_query = new BasicDBObject();
						new_query.put("company_id", company_obj_id);
						new_query.put("shop_id", shop_obj_id);
						
						if (DB_Out_tbl.equalsIgnoreCase("customer_portfolio_orders"))
						{
							for (int i = 0; i < 4; i++) {
								new_query.put(header_parts[i], parts[i]);
							}
							collection.save(new_query);
							
							Date tempDate = new SimpleDateFormat("yyyy/MM/dd").parse(parts[4]);
							new_query.put("order_date", tempDate);
							new_query.put("status_order", parts[5]);
							portfolio_order_collection.save(new_query);
							System.out.println("Create new with cd: " + parts[1]);
						}
						else
						{
							for (int i = 0; i < parts.length; i++) {
								new_query.put(header_parts[i], parts[i]);
							}
							
							if (DB_Out_tbl.equalsIgnoreCase("customers"))
							{
								new_query.put("receive_status", 1);
							}
							System.out.println("Create new with cd: " + parts[0]);
							collection.save(new_query);
						}
					}
				}
				System.out.println("Done");
			}
			
		} catch (NoSuchElementException e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

	protected ArrayList<String> get_recommended_items_by_user(String DB_host,
			int DB_port, String DB_name, String DB_table, String DB_table_out,
			String user_id, int number_of_items) throws Exception {
		ArrayList<String> x = new ArrayList<String>();
		try {

			long start = System.currentTimeMillis();
			System.out.println("Start: " + start);
			MongoDBDataModel model = new MongoDBDataModel(DB_host, DB_port,
					DB_name, DB_table, false, false, null);

			long start_run_CF = System.currentTimeMillis();
			System.out.println("Start Run CF: " + start_run_CF);

			UserSimilarity similarity = new TanimotoCoefficientSimilarity(model);
			UserNeighborhood neighborhood = new NearestNUserNeighborhood(50,
					similarity, model);
			Recommender recommender = new GenericBooleanPrefUserBasedRecommender(
					model, neighborhood, similarity);
			List<RecommendedItem> recommendations;

			// Prepare to add to DB
			BasicDBObject document = new BasicDBObject();
			// List of Recommended Items

			recommendations = recommender.recommend(
					Long.parseLong(model.fromIdToLong(user_id, false)),
					number_of_items);
			for (RecommendedItem recommendation : recommendations) {
				System.out.println(recommendation);
				x.add(model.fromLongToId(recommendation.getItemID()));
			}
			document.put("recommended_items", x);
			document.put("recommended_time", start);
			// collection.save(document);

			System.out.println("Done");
			long end = System.currentTimeMillis();
			System.out.println("Time to read DB: " + (start_run_CF - start));
			System.out.println("Run time in Miliseconds: " + (end - start));

		} catch (UnknownHostException e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
		return x;
	}

	protected void get_recommended_items(String DB_host, int DB_port,
			String DB_name, String DB_table, String DB_table_out,
			int number_of_items) throws Exception {
		try {
			Mongo mongo = new Mongo(DB_host, DB_port); // 27017

			// get database from MongoDB,
			// if database doesn't exists, mongoDB will create it automatically
			DB db = mongo.getDB(DB_name);

			// Get collection from MongoDB, database named "yourDB"
			// if collection doesn't exists, mongoDB will create it
			// automatically
			DBCollection collection = db.getCollection(DB_table_out);

			long start = System.currentTimeMillis();
			System.out.println("Start: " + start);

			// check memory
			long freeMemory = Runtime.getRuntime().freeMemory() / MegaBytes;
			long totalMemory = Runtime.getRuntime().totalMemory() / MegaBytes;
			long maxMemory = Runtime.getRuntime().maxMemory() / MegaBytes;

			System.out.println("JVM freeMemory: " + freeMemory);
			System.out
					.println("JVM totalMemory also equals to initial heap size of JVM : "
							+ totalMemory);
			System.out
					.println("JVM maxMemory also equals to maximum heap size of JVM: "
							+ maxMemory);

			MongoDBDataModel model = new MongoDBDataModel(DB_host, DB_port,
					DB_name, DB_table, false, false, null);

			long start_run_CF = System.currentTimeMillis();
			System.out.println("Start Run CF: " + start_run_CF);
			LongPrimitiveIterator it = model.getUserIDs();

			UserSimilarity similarity = new TanimotoCoefficientSimilarity(model);
			UserNeighborhood neighborhood = new NearestNUserNeighborhood(50,
					similarity, model);
			Recommender recommender = new GenericBooleanPrefUserBasedRecommender(
					model, neighborhood, similarity);
			// String s = "";
			List<RecommendedItem> recommendations;
			// Prepare to add to DB
			BasicDBObject document = new BasicDBObject();
			// List of Recommended Items
			ArrayList<String> x = new ArrayList<String>();
			while (it.hasNext()) {
				long userID = it.nextLong();
				document.clear();
				document.put("user_id", model.fromLongToId(userID));

				recommendations = recommender
						.recommend(userID, number_of_items);
				// s = s + "\n" + String.valueOf(userID) + "\n";
				if (recommendations.isEmpty()) {
					// s = s + "\t no recommendedItem";
				} else {
					x.clear();
					for (RecommendedItem recommendation : recommendations) {
						// s = s
						// + "\t"
						// + (String.valueOf(model
						// .fromLongToId(recommendation
						// .getItemID())));
						x.add(model.fromLongToId(recommendation.getItemID()));
					}
				}
				document.put("recommended_items", x);
				System.out.println(x);
				document.put("recommended_time", start);
				collection.save(document);
			}
			System.out.println("Done");
			long end = System.currentTimeMillis();
			System.out.println("Time to read DB: " + (start_run_CF - start));
			System.out.println("Run time in Miliseconds: " + (end - start));

			// check memory
			freeMemory = Runtime.getRuntime().freeMemory() / MegaBytes;
			totalMemory = Runtime.getRuntime().totalMemory() / MegaBytes;
			maxMemory = Runtime.getRuntime().maxMemory() / MegaBytes;

			System.out.println("Used Memory in JVM: "
					+ (maxMemory - freeMemory));
			System.out.println("freeMemory in JVM: " + freeMemory);
			System.out
					.println("totalMemory in JVM shows current size of java heap : "
							+ totalMemory);
			System.out.println("maxMemory in JVM: " + maxMemory);

		} catch (UnknownHostException e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

	// Recommendation from File CSV
	protected ArrayList<String> get_recommended_items_by_user_from_file(
			String DB_host, int DB_port, String DB_name, String file_path,
			String DB_table_out, long user_id, int number_of_items)
			throws Exception {
		ArrayList<String> x = new ArrayList<String>();
		try {
			Mongo mongo = new Mongo(DB_host, DB_port);

			// get database from MongoDB,
			// if database doesn't exists, mongoDB will create it automatically
			DB db = mongo.getDB(DB_name);

			// Get collection from MongoDB, database named "yourDB"
			// if collection doesn't exists, mongoDB will create it
			// automatically
			DBCollection collection = db.getCollection(DB_table_out);

			long start = System.currentTimeMillis();
			System.out.println("Start: " + start);
			DataModel model = new FileDataModel(new File(file_path));

			long start_run_CF = System.currentTimeMillis();
			System.out.println("Start Run CF: " + start_run_CF);

			UserSimilarity similarity = new TanimotoCoefficientSimilarity(model);
			UserNeighborhood neighborhood = new NearestNUserNeighborhood(50,
					similarity, model);
			Recommender recommender = new GenericBooleanPrefUserBasedRecommender(
					model, neighborhood, similarity);
			String s = "";
			List<RecommendedItem> recommendations;
			// Prepare to add to DB
			BasicDBObject document = new BasicDBObject();
			document.put("user_id", user_id);
			// List of Recommended Items

			recommendations = recommender.recommend(user_id, number_of_items);
			s = s + "\n" + String.valueOf(user_id) + "\n";
			if (recommendations.isEmpty()) {
				s = s + "\t no recommendedItem";
			} else {
				for (RecommendedItem recommendation : recommendations) {
					s = s + "\t" + (String.valueOf(recommendation.getItemID()));
					x.add(Long.toString(recommendation.getItemID()));
				}
			}
			document.put("recommended_items", x);
			document.put("recommended_time", start);
			collection.save(document);
			System.out.println(x);
			System.out.println("Done");
			long end = System.currentTimeMillis();
			System.out.println("Time to read DB: " + (start_run_CF - start));
			System.out.println("Run time in Miliseconds: " + (end - start));
		} catch (UnknownHostException e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
		return x;
	}

	protected void get_recommended_items_from_file(String DB_host, int DB_port,
			String DB_name, String file_path, String DB_table_out,
			int number_of_items) throws Exception {
		try {
			Mongo mongo = new Mongo(DB_host, DB_port);

			// get database from MongoDB,
			// if database doesn't exists, mongoDB will create it automatically
			DB db = mongo.getDB(DB_name);

			// Get collection from MongoDB, database named "yourDB"
			// if collection doesn't exists, mongoDB will create it
			// automatically
			DBCollection collection = db.getCollection(DB_table_out);

			long start = System.currentTimeMillis();
			System.out.println("Start: " + start);
			DataModel model = new FileDataModel(new File(file_path));

			long start_run_CF = System.currentTimeMillis();
			System.out.println("Start Run CF: " + start_run_CF);
			LongPrimitiveIterator it = model.getUserIDs();

			UserSimilarity similarity = new TanimotoCoefficientSimilarity(model);
			UserNeighborhood neighborhood = new NearestNUserNeighborhood(50,
					similarity, model);
			Recommender recommender = new GenericBooleanPrefUserBasedRecommender(
					model, neighborhood, similarity);
			String s = "";
			List<RecommendedItem> recommendations;
			while (it.hasNext()) {
				long userID = it.nextLong();
				// Prepare to add to DB
				BasicDBObject document = new BasicDBObject();
				document.put("user_id", userID);
				// List of Recommended Items
				ArrayList<String> x = new ArrayList<String>();
				recommendations = recommender
						.recommend(userID, number_of_items);
				s = s + "\n" + String.valueOf(userID) + "\n";
				if (recommendations.isEmpty()) {
					s = s + "\t no recommendedItem";
				} else {
					for (RecommendedItem recommendation : recommendations) {
						s = s + "\t"
								+ (String.valueOf(recommendation.getItemID()));
						x.add(Long.toString(recommendation.getItemID()));
					}
				}
				document.put("recommended_items", x);
				document.put("recommended_time", start);
				collection.save(document);
			}
			System.out.println("Done");
			long end = System.currentTimeMillis();
			System.out.println("Time to read DB: " + (start_run_CF - start));
			System.out.println("Run time in Miliseconds: " + (end - start));
		} catch (UnknownHostException e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

	private static final int MegaBytes = 10241024;
	private BufferedReader in;

	// Main function
	protected void get_recommended_items(String DB_host, int DB_port,
			String DB_name, String DB_table, String DB_table_out,
			int number_of_items, String userIDField, String itemIDField, String shop_id,
			String company_id) throws Exception {
		try {
			// check memory
			long freeMemory = Runtime.getRuntime().freeMemory() / MegaBytes;
			long totalMemory = Runtime.getRuntime().totalMemory() / MegaBytes;
			long maxMemory = Runtime.getRuntime().maxMemory() / MegaBytes;

			System.out.println("JVM freeMemory: " + freeMemory);
			System.out
					.println("JVM totalMemory also equals to initial heap size of JVM : "
							+ totalMemory);
			System.out
					.println("JVM maxMemory also equals to maximum heap size of JVM: "
							+ maxMemory);

			Mongo mongo = new Mongo(DB_host, DB_port); // 27017

			// get database from MongoDB,
			// if database doesn't exists, mongoDB will create it automatically
			DB db = mongo.getDB(DB_name);

			// Get collection from MongoDB, database named "yourDB"
			// if collection doesn't exists, mongoDB will create it
			// automatically
			DBCollection collection = db.getCollection(DB_table_out);

			long start = System.currentTimeMillis();
			System.out.println("Start: " + start);
			MongoDBDataModelExtended model = new MongoDBDataModelExtended(
					DB_host, DB_port, DB_name, DB_table, false, false, null,
					userIDField, itemIDField, company_id);

			long start_run_CF = System.currentTimeMillis();
			System.out.println("Start Run CF: " + start_run_CF);
			LongPrimitiveIterator it = model.getUserIDs();
			UserSimilarity similarity = new TanimotoCoefficientSimilarity(model);
			UserNeighborhood neighborhood = new NearestNUserNeighborhood(50,
					similarity, model);
			Recommender recommender = new GenericBooleanPrefUserBasedRecommender(
					model, neighborhood, similarity);
			List<RecommendedItem> recommendations;
			// Prepare to add to DB
			BasicDBObject document = new BasicDBObject();
			// List of Recommended Items
			ArrayList<String> x = new ArrayList<String>();
			while (it.hasNext()) {
				long userID = it.nextLong();
				document.clear();
				document.put("company_customer_id", model.fromLongToId(userID));

				recommendations = recommender
						.recommend(userID, number_of_items);
				x.clear();
				if (recommendations.isEmpty()) {
				} else {

					for (RecommendedItem recommendation : recommendations) {
						x.add(model.fromLongToId(recommendation.getItemID()));
					}
				}
				document.put("recommended_items", x);
				ObjectId company_obj_id = new ObjectId(company_id);
				ObjectId shop_obj_id = new ObjectId(shop_id);
				document.put("company_id", company_obj_id);
				document.put("shop_id", shop_obj_id);
				document.put("recommended_time", start);
				collection.save(document);
			}
			System.out.println("Done");
			long end = System.currentTimeMillis();
			System.out.println("Time to read DB: " + (start_run_CF - start));
			System.out.println("Run time in Miliseconds: " + (end - start));

			// check memery
			freeMemory = Runtime.getRuntime().freeMemory() / MegaBytes;
			totalMemory = Runtime.getRuntime().totalMemory() / MegaBytes;
			maxMemory = Runtime.getRuntime().maxMemory() / MegaBytes;

			System.out.println("Used Memory in JVM: "
					+ (maxMemory - freeMemory));
			System.out.println("freeMemory in JVM: " + freeMemory);
			System.out
					.println("totalMemory in JVM shows current size of java heap : "
							+ totalMemory);
			System.out.println("maxMemory in JVM: " + maxMemory);

		} catch (UnknownHostException e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

	// insert recommended items in Customer table
	protected void insert_recommended_items(String DB_host, int DB_port,
			String DB_name, String DB_table, String DB_table_out,
			int number_of_items, String userIDField, String itemIDField,
			String company_id) throws Exception {
		try {
			// check memory
			long freeMemory = Runtime.getRuntime().freeMemory() / MegaBytes;
			long totalMemory = Runtime.getRuntime().totalMemory() / MegaBytes;
			long maxMemory = Runtime.getRuntime().maxMemory() / MegaBytes;

			System.out.println("JVM freeMemory: " + freeMemory);
			System.out
					.println("JVM totalMemory also equals to initial heap size of JVM : "
							+ totalMemory);
			System.out
					.println("JVM maxMemory also equals to maximum heap size of JVM: "
							+ maxMemory);

			Mongo mongo = new Mongo(DB_host, DB_port); // 27017

			// get database from MongoDB,
			// if database doesn't exists, mongoDB will create it automatically
			DB db = mongo.getDB(DB_name);

			// Get collection from MongoDB, database named "yourDB"
			// if collection doesn't exists, mongoDB will create it
			// automatically
			DBCollection collection = db.getCollection(DB_table_out);

			long start = System.currentTimeMillis();
			System.out.println("Start: " + start);
			MongoDBDataModelExtended model = new MongoDBDataModelExtended(
					DB_host, DB_port, DB_name, DB_table, false, false, null,
					userIDField, itemIDField, company_id);

			long start_run_CF = System.currentTimeMillis();
			System.out.println("Start Run CF: " + start_run_CF);
			LongPrimitiveIterator it = model.getUserIDs();

			UserSimilarity similarity = new TanimotoCoefficientSimilarity(model);
			UserNeighborhood neighborhood = new NearestNUserNeighborhood(50,
					similarity, model);
			Recommender recommender = new GenericBooleanPrefUserBasedRecommender(
					model, neighborhood, similarity);
			List<RecommendedItem> recommendations;
			// Prepare to add to DB
			BasicDBObject document = new BasicDBObject();
			ObjectId company_obj_id = new ObjectId(company_id);
			// List of Recommended Items
			ArrayList<String> x = new ArrayList<String>();
			while (it.hasNext()) {
				long userID = it.nextLong();
				document.clear();

				BasicDBObject searchQuery = new BasicDBObject();
				searchQuery.put("company_customer_id",
						model.fromLongToId(userID));
				searchQuery.put("company_id", company_obj_id);

				// document.put("company_customer_id",
				// model.fromLongToId(userID));

				recommendations = recommender
						.recommend(userID, number_of_items);
				x.clear();
				if (recommendations.isEmpty()) {
				} else {

					for (RecommendedItem recommendation : recommendations) {
						x.add(model.fromLongToId(recommendation.getItemID()));
					}
				}
				int xx = x.size();

				BasicDBObject newDocument = new BasicDBObject();
				newDocument.put("recommended_items", x);
				newDocument.put("no_of_items", xx);
				newDocument.put("recommended_time", start);

				BasicDBObject updateObj = new BasicDBObject();
				updateObj.put("$set", newDocument);

				collection.update(searchQuery, updateObj, false, true);
			}
			System.out.println("Done");
			long end = System.currentTimeMillis();
			System.out.println("Time to read DB: " + (start_run_CF - start));
			System.out.println("Run time in Miliseconds: " + (end - start));

			// check memery
			freeMemory = Runtime.getRuntime().freeMemory() / MegaBytes;
			totalMemory = Runtime.getRuntime().totalMemory() / MegaBytes;
			maxMemory = Runtime.getRuntime().maxMemory() / MegaBytes;

			System.out.println("Used Memory in JVM: "
					+ (maxMemory - freeMemory));
			System.out.println("freeMemory in JVM: " + freeMemory);
			System.out
					.println("totalMemory in JVM shows current size of java heap : "
							+ totalMemory);
			System.out.println("maxMemory in JVM: " + maxMemory);

		} catch (UnknownHostException e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

	// Create customer sort table
	protected void create_customer_sort_table(String DB_host, int DB_port,
			String DB_name, String DB_cust_table, String DB_rec_table, String DB_table_out,String shop_id, String company_id) throws Exception {
		try {
			Mongo mongo = new Mongo(DB_host, DB_port);

			DB db = mongo.getDB(DB_name);

			DBCollection collection = db.getCollection(DB_cust_table);
			BasicDBObject query = new BasicDBObject();

			ObjectId company_obj_id = new ObjectId(company_id);
			ObjectId shop_obj_id = new ObjectId(shop_id);
			
			query.put("company_id", company_obj_id);
			query.put("shop_id", shop_obj_id);
			DBCursor cursor = collection.find(query);
			while (cursor.hasNext()) {
				BasicDBObject result = (BasicDBObject) cursor.next();
				
				ObjectId customer_id = (ObjectId) result.get("_id");
				String company_cust_id = (String) result.get("company_customer_id");
				String customer_name = (String) result.get("customer_name");
				String customer_email = (String) result.get("email");
				Integer delivered = (Integer) result.get("delivered");
				Integer receive_status = (Integer) result.get("receive_status");
				
				// Create new collection
				DBCollection collection_customer_sort = db.getCollection(DB_table_out);
				BasicDBObject document = new BasicDBObject();
				document.put("company_id", company_obj_id);
				document.put("shop_id", shop_obj_id);
				document.put("customer_id", customer_id.toString());
				document.put("company_customer_id", company_cust_id);
				document.put("customer_name", customer_name);
				document.put("email", customer_email);
				document.put("delivered", delivered);
				document.put("receive_status", receive_status);
				
				// Find customer recommended items
				DBCollection customer_recommeded_items = db.getCollection(DB_rec_table);
				BasicDBObject cust_rec_query = new BasicDBObject();
				cust_rec_query.put("company_customer_id", company_cust_id);
				cust_rec_query.put("company_id", company_obj_id);
				cust_rec_query.put("shop_id", shop_obj_id);
				BasicDBObject recommended_items = (BasicDBObject) customer_recommeded_items.findOne(cust_rec_query);
				
				if (recommended_items == null)
				{
					document.put("no_of_items", 0);
					document.put("recommended_items", new ArrayList<Integer> ());
				}
				else
				{
					BasicDBList features = (BasicDBList) recommended_items.get("recommended_items");
					if (features != null) {
						document.put("recommended_items", features);
						document.put("no_of_items", features.size());
					} else {
						document.put("recommended_items", new ArrayList<Integer> ());
						document.put("no_of_items", 0);
					}
				}
				collection_customer_sort.save(document);
			}
		} catch (UnknownHostException e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

}