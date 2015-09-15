package cf;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import org.apache.mahout.cf.taste.impl.common.FastIDSet;
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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
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

public class ClusterRecommendation {
	protected void get_clusters(String DB_host, int DB_port, String DB_name,
			String DB_table, String DB_table_out, int number_of_items,
			String userIDField, String itemIDField, String shop_id, String company_id)
			throws Exception {
		try {
			long start = System.currentTimeMillis();
			System.out.println("Start:" + start);

			Mongo mongo = new Mongo(DB_host, DB_port); // 27017

			DB db = mongo.getDB(DB_name);

			DBCollection collection = db.getCollection(DB_table_out);

			MongoDBDataModelExtended model = new MongoDBDataModelExtended(
					DB_host, DB_port, DB_name, DB_table, false, false, null,
					userIDField, itemIDField, company_id);

			UserSimilarity similarity = new TanimotoCoefficientSimilarity(model);
			@SuppressWarnings("deprecation")
			ClusterSimilarity clusterSimilarity = new FarthestNeighborClusterSimilarity(
					similarity);
			System.out.println("aaa");
			@SuppressWarnings("deprecation")
			TreeClusteringRecommender2 recommender = new TreeClusteringRecommender2(
					model, clusterSimilarity, 40);
			System.out.println("bbb");

			// Remove current data
			BasicDBObject query = new BasicDBObject();
			ObjectId company_obj_id = new ObjectId(company_id);
			ObjectId shop_obj_id = new ObjectId(shop_id);
			query.put("company_id", company_obj_id);
			query.put("shop_id", shop_obj_id);
			collection.remove(query);

			@SuppressWarnings("deprecation")
			FastIDSet[] fis = recommender.getClusters();
			System.out.println(fis);
			for (FastIDSet fii : fis) {
				System.out.println(fii);
				// Change long to ID
				long[] cluster_array = fii.toArray();
				long[] new_array = new long[cluster_array.length];
				for (int i = 0; i < cluster_array.length; i++) {
					String idStr = model.fromLongToId(cluster_array[i]);
					long id = Integer.parseInt(idStr);
					new_array[i] = id;
				}
				BasicDBObject document = new BasicDBObject();
				document.put("cluster_array", new_array);
				document.put("company_id", company_obj_id);
				document.put("shop_id", shop_obj_id);
				collection.save(document);
			}
		} catch (UnknownHostException e) {
			System.out.println("exception");
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

	protected void insert_order(String DB_host, int DB_port, String DB_name,
			String DB_table, String DB_table_out, long company_customer_id,
			String userIDField, String itemIDField, String shop_id, String company_id)
			throws Exception {
		try {
			long start = System.currentTimeMillis();
			System.out.println("Start:" + start);

			@SuppressWarnings("deprecation")
			Mongo mongo = new Mongo(DB_host, DB_port); // 27017

			DB db = mongo.getDB(DB_name);

			MongoDBDataModelExtended model = new MongoDBDataModelExtended(
					DB_host, DB_port, DB_name, DB_table, false, false, null,
					userIDField, itemIDField, company_id);

			UserSimilarity similarity = new TanimotoCoefficientSimilarity(model);
			@SuppressWarnings("deprecation")
			ClusterSimilarity clusterSimilarity = new FarthestNeighborClusterSimilarity(
					similarity);

			ObjectId company_obj_id = new ObjectId(company_id);
			ObjectId shop_obj_id = new ObjectId(shop_id);
			BasicDBObject searchQuery = new BasicDBObject();
			searchQuery.put("company_id", company_obj_id);
			searchQuery.put("shop_id", shop_obj_id);
			DBCollection collection = db.getCollection(DB_table_out);
			DBCursor data = collection.find(searchQuery);

			double maxCompareValue = 0;
			Object arr_data = null;
			ObjectId cluster_id_found = new ObjectId();

			while (data.hasNext()) {
				DBObject row = data.next();
				BasicDBList cluster_arr = (BasicDBList) row
						.get("cluster_array");
				long[] arr = new long[cluster_arr.size()];
				ArrayList<Long> arr_list = new ArrayList<Long>();

				FastIDSet fi1 = new FastIDSet();

				for (int i = 0; i < cluster_arr.size(); i++) {
					arr[i] = (long) Integer.parseInt(cluster_arr.get(i)
							.toString());
					arr_list.add(arr[i]);

					// Get long id from model. Because model mapped long id to
					// model id, 2 values are difference
					fi1.add((long) Integer.parseInt(model.fromIdToLong(
							Long.toString(arr[i]), true)));
				}
				System.out.println(fi1);
				FastIDSet fi2 = new FastIDSet();
				fi2.add((long) Integer.parseInt(model.fromIdToLong(
						Long.toString(company_customer_id), true)));
				System.out.println(fi2);
				// Compare 2 object
				@SuppressWarnings("deprecation")
				double compareValue = clusterSimilarity.getSimilarity(fi1, fi2);

				// Get max compare value
				if (maxCompareValue < compareValue) {
					maxCompareValue = compareValue;
					ObjectId current_cluster_id = (ObjectId) row.get("_id");
					cluster_id_found = new ObjectId(
							current_cluster_id.toString());

					arr_list.add(company_customer_id);
					arr_data = arr_list;
				}
			}

			// Insert data to cluster
			BasicDBObject search = new BasicDBObject();
			search.put("_id", cluster_id_found);
			search.put("company_id", company_obj_id);

			BasicDBObject updateQuery = new BasicDBObject();

			updateQuery.put("cluster_array", arr_data);
			updateQuery.put("company_id", company_obj_id);
			System.out.println(arr_data);
			// Update
			collection.update(search, updateQuery);
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

	protected void update_order(String DB_host, int DB_port, String DB_name,
			String DB_table, String DB_table_out, long company_customer_id,
			String userIDField, String itemIDField, String company_id)
			throws Exception {
		try {
			long start = System.currentTimeMillis();
			System.out.println("Start:" + start);

			@SuppressWarnings("deprecation")
			Mongo mongo = new Mongo(DB_host, DB_port); // 27017

			DB db = mongo.getDB(DB_name);

			MongoDBDataModelExtended model = new MongoDBDataModelExtended(
					DB_host, DB_port, DB_name, DB_table, false, false, null,
					userIDField, itemIDField, company_id);

			UserSimilarity similarity = new TanimotoCoefficientSimilarity(model);
			@SuppressWarnings("deprecation")
			ClusterSimilarity clusterSimilarity = new FarthestNeighborClusterSimilarity(
					similarity);
			
			
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}
}
