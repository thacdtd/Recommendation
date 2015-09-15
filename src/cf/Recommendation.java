package cf;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.*;
import org.apache.mahout.cf.taste.impl.neighborhood.*;
import org.apache.mahout.cf.taste.impl.recommender.*;
import org.apache.mahout.cf.taste.impl.similarity.*;
import org.apache.mahout.cf.taste.model.*;
import org.apache.mahout.cf.taste.neighborhood.*;
import org.apache.mahout.cf.taste.recommender.*;
import org.apache.mahout.cf.taste.similarity.*;

import com.ibm.icu.text.SimpleDateFormat;

import java.io.*;
import java.util.*;

class Recommendation {

  private Recommendation() {
  }

  public static void main(String[] args) throws Exception {
//	Date dNow = new Date( );
//	SimpleDateFormat ft = new SimpleDateFormat ("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
	
//	System.out.println("Current Date: " + ft.format(dNow));
	long start = System.currentTimeMillis();
//	System.out.println("Start: " + start);

    File file = new File("newfile5.txt");
	String s = "";

//	try (FileOutputStream fop = new FileOutputStream(file)) {

		// if file doesn't exists, then create it
//		if (!file.exists()) {
//			file.createNewFile();
//		}
		DataModel model = new FileDataModel(new File("test.csv"));

	    UserSimilarity similarity = new TanimotoCoefficientSimilarity(model);
	    UserNeighborhood neighborhood = new NearestNUserNeighborhood(500, similarity, model);
//		List<RecommendedItem> recommendations;
//		LongPrimitiveIterator it = model.getUserIDs();
		Recommender recommender = new GenericBooleanPrefUserBasedRecommender(model, neighborhood, similarity);
//		while (it.hasNext()) {
//			long userID = it.nextLong();
//			recommendations = recommender.recommend(userID, 5);
//			s = s + "\n" + String.valueOf(userID) + "\n";
//			if (recommendations.isEmpty()){
//				s = s + "\t no recommendedItem";
//			}
//			else {
//				for (RecommendedItem recommendation : recommendations) {
//					s = s + "\t" + (String.valueOf(recommendation.getItemID())); //+ ", " + String.valueOf(recommendation.getValue());
//				    System.out.println(recommendation);
//				}
//			}
//		}
		
		List<RecommendedItem> recommendations = recommender.recommend(8, 5);
		for (RecommendedItem recommendation : recommendations) {
			s = s + "\n" + (String.valueOf(recommendation.getItemID())) + ", " + String.valueOf(recommendation.getValue());
		    System.out.println(recommendation);
		}
		// get the content in bytes
		byte[] contentInBytes = s.getBytes();

//		fop.write(contentInBytes);
//		fop.flush();
//		fop.close();

		System.out.println("Done");

//	} catch (IOException e) {
//		e.printStackTrace();
//	}
	
//	Date dNow2 = new Date( );
//
//	System.out.println("Current Date: " + ft.format(dNow2));
	long end = System.currentTimeMillis();
	System.out.println("Run time in Miliseconds: " + (end-start));
	
  }

}