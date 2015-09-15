package CollaborativeFiltering;

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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import cf.MongoDBDataModelExtended;

class SimpleRecommendation {

	public SimpleRecommendation() {
	}

	public static void main(String[] args) throws Exception {
	}
	
	protected byte[] create_Hash_key(String word) {
//		ArrayList<String> list = new ArrayList<String> ();
		byte[] bb = null;
		try {
			MessageDigest md;
			md = MessageDigest.getInstance("MD5");

		//List<byte[]> hashes = new ArrayList<byte[]>(list.size());
			//hashes.add(md.digest(word.getBytes("UTF-16LE")));
			bb = md.digest(word.getBytes("UTF-16LE"));
		} catch (NoSuchAlgorithmException e1) {
			e1.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		System.out.println(bb);
		return bb;
	}
	
	protected void delete_DB(String DB_host,
			int DB_port, String DB_name, String DB_Out_tbl, String company_id) throws Exception {
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
			ObjectId company_obj_id= new ObjectId(company_id);
			query.put("company_id", company_obj_id);
			collection.remove(query);
		} catch (UnknownHostException e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}
	
	protected void import_CSV(String DB_host,
			int DB_port, String DB_name, String file_input, String DB_Out_tbl, String company_id) throws Exception {
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
			ObjectId company_obj_id= new ObjectId(company_id);
			query.put("company_id", company_obj_id);
			collection.remove(query);
			
			File fleExample = new File(file_input);
			
			// Find out if the file exists already
			if (fleExample.exists()) {
				BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(fleExample), "UTF-8"));
				// Prepare a Scanner that will "scan" the document
				// Scanner opnScanner = new Scanner(fleExample);
				// Read each line in the file
				// String temp = "";
				// System.out.println(opnScanner.hasNext());
				// temp = opnScanner.nextLine();
				String str = in.readLine();
			            
				String[] header_parts = str.split(",");
				
				BasicDBObject document = new BasicDBObject();
				while ((str = in.readLine()) != null){
					// temp = opnScanner.nextLine();
					document.clear();
					// Read each line and display its value
					String[] parts = str.split(",");
					for (int i = 0; i < parts.length; i++) {
						System.out.println(header_parts[i]);
						System.out.println(parts[i]);
//						byte ptext[] = parts[i].getBytes("UTF-8"); 
//						String value = new String(ptext, "Shift_JIS"); 
						document.put(header_parts[i], parts[i]);
					}
					//ObjectId company_obj_id= new ObjectId(company_id);
					document.put("company_id", company_obj_id);
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
					Long.parseLong(model.fromIdToLong(user_id, false)), number_of_items);
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
			String DB_name, String DB_table, String DB_table_out, int number_of_items)
			throws Exception {
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
			
			//check memory
			long freeMemory = Runtime.getRuntime().freeMemory()/MegaBytes;
            long totalMemory = Runtime.getRuntime().totalMemory()/MegaBytes;
            long maxMemory = Runtime.getRuntime().maxMemory()/MegaBytes;

            System.out.println("JVM freeMemory: " + freeMemory);
            System.out.println("JVM totalMemory also equals to initial heap size of JVM : "
                                       + totalMemory);
            System.out.println("JVM maxMemory also equals to maximum heap size of JVM: "
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
//			String s = "";
			List<RecommendedItem> recommendations;
			// Prepare to add to DB
			BasicDBObject document = new BasicDBObject();
			// List of Recommended Items
			ArrayList<String> x = new ArrayList<String>();
			while (it.hasNext()) {
				long userID = it.nextLong();
				document.clear();
				document.put("user_id", model.fromLongToId(userID));
				
				recommendations = recommender.recommend(userID, number_of_items);
//				s = s + "\n" + String.valueOf(userID) + "\n";
				if (recommendations.isEmpty()) {
//					s = s + "\t no recommendedItem";
				} else {
					x.clear();
					for (RecommendedItem recommendation : recommendations) {
//						s = s
//								+ "\t"
//								+ (String.valueOf(model
//										.fromLongToId(recommendation
//												.getItemID())));
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
			
			//check memory
			freeMemory = Runtime.getRuntime().freeMemory() / MegaBytes;
            totalMemory = Runtime.getRuntime().totalMemory() / MegaBytes;
            maxMemory = Runtime.getRuntime().maxMemory() / MegaBytes;

            System.out.println("Used Memory in JVM: " + (maxMemory - freeMemory));
            System.out.println("freeMemory in JVM: " + freeMemory);
            System.out.println("totalMemory in JVM shows current size of java heap : "
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
			String DB_name, String file_path, String DB_table_out, int number_of_items)
			throws Exception {
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
				recommendations = recommender.recommend(userID, number_of_items);
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
	
	//Test
	protected void get_recommended_items(String DB_host, int DB_port,
			String DB_name, String DB_table, String DB_table_out, int number_of_items, String userIDField, String itemIDField, String company_id)
			throws Exception {
		try {
			//check memory
			long freeMemory = Runtime.getRuntime().freeMemory()/MegaBytes;
            long totalMemory = Runtime.getRuntime().totalMemory()/MegaBytes;
            long maxMemory = Runtime.getRuntime().maxMemory()/MegaBytes;

            System.out.println("JVM freeMemory: " + freeMemory);
            System.out.println("JVM totalMemory also equals to initial heap size of JVM : "
                                       + totalMemory);
            System.out.println("JVM maxMemory also equals to maximum heap size of JVM: "
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
			MongoDBDataModelExtended model = new MongoDBDataModelExtended(DB_host, DB_port,
					DB_name, DB_table, false, false, null, userIDField, itemIDField, company_id); 
			
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
				
				recommendations = recommender.recommend(userID, number_of_items);
				x.clear();
				if (recommendations.isEmpty()) {
				} else {
					
					for (RecommendedItem recommendation : recommendations) {
						x.add(model.fromLongToId(recommendation.getItemID()));
					}
				}
				document.put("recommended_items", x);
				ObjectId company_obj_id= new ObjectId(company_id);
				document.put("company_id", company_obj_id);
				document.put("recommended_time", start);
				collection.save(document);
			}
			System.out.println("Done");
			long end = System.currentTimeMillis();
			System.out.println("Time to read DB: " + (start_run_CF - start));
			System.out.println("Run time in Miliseconds: " + (end - start));
			
			//check memery
			freeMemory = Runtime.getRuntime().freeMemory() / MegaBytes;
            totalMemory = Runtime.getRuntime().totalMemory() / MegaBytes;
            maxMemory = Runtime.getRuntime().maxMemory() / MegaBytes;

            System.out.println("Used Memory in JVM: " + (maxMemory - freeMemory));
            System.out.println("freeMemory in JVM: " + freeMemory);
            System.out.println("totalMemory in JVM shows current size of java heap : "
                                       + totalMemory);
            System.out.println("maxMemory in JVM: " + maxMemory);
	
		} catch (UnknownHostException e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}
	
}