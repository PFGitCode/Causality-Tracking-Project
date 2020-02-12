package DataCompress;	
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.*;
import java.util.ArrayList;
import static java.lang.Math.min;
import java.util.*;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSetMetaData;

public class Query{
	public static int totalTime = 0;
	public static int unDecompressTime = 0;
	public Map<Integer, Set<Integer>> nodes2Newnode;
	public Map<Integer, Set<Integer>> newnode2node ;
	public void setNodesMapping(Map<Integer, Set<Integer>> nodes2Newnode, Map<Integer, Set<Integer>> newnode2node){
		this.nodes2Newnode = nodes2Newnode;
		this.newnode2node = newnode2node;
	}	

	public ArrayList<String> originQuery(String endTime,String dstId, Statement statement){
		ResultSet rs = null;
		ArrayList<String> ansList = new ArrayList();
		try{
			String query = "select * from fileevent where dstid=" + dstId + " and starttime < " + endTime + ";";
			if(statement.execute(query))
				rs = statement.getResultSet();
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnsNumber = rsmd.getColumnCount();
			while(rs.next()){
				String ans = rs.getString(2)+","+rs.getString(7);
				ansList.add(ans);
			}
		}catch(Exception exp){
			exp.printStackTrace(); 
       		System.out.println(exp); 
		}
	       	return ansList;
	}	

	public ArrayList<String> singleQuery(String endTime,String dstId, Statement statement){
		ResultSet rs = null;
		int dstIdInt = Integer.parseInt(dstId);
		ArrayList<String> ansList = new ArrayList();
		try{
			String query = "select * from fileevent where dstid=" + dstId + ";";
			if(statement.execute(query))
				rs = statement.getResultSet();
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnsNumber = rsmd.getColumnCount();
			while(rs.next()){
				String srcidStr = rs.getString(7);
				int srcidInt = Integer.parseInt(srcidStr);
				// System.out.println(srcidInt);
				if(srcidInt > 0){
					Long starttime = Long.parseLong(rs.getString(2));
					if(starttime < Long.parseLong(endTime)){
						String ans = rs.getString(2)+","+rs.getString(7);
						ansList.add(ans);
					}
				}
				else{
					Set<Integer> nodes = newnode2node.get(srcidInt);

					String starttimeStr = rs.getString(2);
					String starttime_seqStr = rs.getString(3);
					String endtimeStr = rs.getString(4);
					String endtime_seqStr = null;

					// if (Long.parseLong(starttimeStr) <= Long.parseLong(endTime)) {
					// 	unDecompressTime++;
						
					// 	return ansList;
					// }
					// else if(Long.parseLong(endtimeStr) >= Long.parseLong(endTime)){
					// 	unDecompressTime++;
					// 	return;
					// }

					String[] timeStrs = rs.getString(5).split(";");
					if(timeStrs.length > 1){
						starttimeStr = starttimeStr + " " + timeStrs[1];
						starttime_seqStr = starttime_seqStr + " " + timeStrs[2];
						endtimeStr = endtimeStr + " " + timeStrs[3];
						endtimeStr = timeStrs[0] + " " + timeStrs[4];
					}
					else{
						endtimeStr = timeStrs[0];
					}

					String[] starttimes = starttimeStr.split(" ");
					String[] starttimeSeqs = starttime_seqStr.split(" ");
					String[] endtimes = endtimeStr.split(" ");
					String[] endtimeSeqes = endtimeStr.split(" ");

					String[] starttimesRes = decode(starttimes);
   					String[] starttimeSeqsRes = decode(starttimeSeqs);
   					String[] endtimesRes = decode(endtimes);
					String[] endtimeSeqesRes = decode(endtimeSeqes);

					int nodeIdx = 1;
					// System.out.println(srcidInt);
					// System.out.println(nodes.size());
					for(int node : nodes){
						 // System.out.println(node);
						if(isInteger(starttimes[nodeIdx])){// if not a merged node, check time
 							if(Long.parseLong(starttimesRes[nodeIdx]) < Long.parseLong(endTime)){
 								// System.out.println("check");
 								String ans = starttimesRes[nodeIdx] + "," + Integer.toString(node);
 								ansList.add(ans);
 							}
 						}
 						else{
 							starttimes[nodeIdx] = starttimes[nodeIdx].replaceAll("\\(","");
 							starttimeSeqs[nodeIdx] = starttimeSeqs[nodeIdx].replaceAll("\\(","");
 							endtimes[nodeIdx] = endtimes[nodeIdx].replaceAll("\\(","");
 							endtimeSeqes[nodeIdx] = endtimeSeqes[nodeIdx].replaceAll("\\(","");

 							starttimes[nodeIdx] = starttimes[nodeIdx].replaceAll("\\)","");
 							starttimeSeqs[nodeIdx] = starttimeSeqs[nodeIdx].replaceAll("\\)","");
 							endtimes[nodeIdx] = endtimes[nodeIdx].replaceAll("\\)","");
 							endtimeSeqes[nodeIdx] = endtimeSeqes[nodeIdx].replaceAll("\\)","");

 	 						String[] starttimesMerged = starttimes[nodeIdx].split("\\.");
 							String[] starttimeSeqsMerged = starttimeSeqs[nodeIdx].split("\\.");
							String[] endtimesMerged = endtimes[nodeIdx].split("\\.");
 							String[] endtimeSeqesMerged = endtimeSeqes[nodeIdx].split("\\.");

 							starttimesMerged[0] = starttimesRes[nodeIdx];
 							starttimeSeqsMerged[0] = starttimeSeqsRes[nodeIdx];
 							endtimesMerged[0] = endtimesRes[nodeIdx];
 							endtimeSeqesMerged[0] = endtimeSeqesRes[nodeIdx];

							String[] s1 = Delta.decode(starttimesMerged);
							String[] s2 = Delta.decode(starttimeSeqsMerged);
	 						String[] s3 = Delta.decode(endtimesMerged);
	 						String[] s4 = Delta.decode(endtimeSeqesMerged);

	 						for(int j = 0; j < s1.length; j++){
	 						if(Long.parseLong(s1[j]) < Long.parseLong(endTime)){
 								String ans = s1[j] + "," + Integer.toString(node);
 								ansList.add(ans);
							}
 						}
					}
					nodeIdx++;
				}
			}
		}
	}catch(Exception exp){
		exp.printStackTrace(); 
       	System.out.println(exp);
       	System.exit(1); 
	}
	// 	else{
	// 		Set<Integer> newNodeSet = nodes2Newnode.get(dstIdInt);
	// 		for(int newNode:newNodeSet){
	// 		String query = "select * from fileevent where dstid=" + newNode + ";";
	// 		System.out.println(newNode);
	// 		if(statement.execute(query)){
	// 			rs = statement.getResultSet();
	// 		}
	// 		ResultSetMetaData rsmd = rs.getMetaData();
	// 		int columnsNumber = rsmd.getColumnCount();
	// 		int pos = 0;
	// 		Set<Integer> nodes = newnode2node.get(newNode);
	// 		for(int n : nodes){
	// 			if(n == dstIdInt)
	// 				break;
	// 			pos++;
	// 		}
	// 		String time = null;
	// 		while(rs.next()){
	// 			boolean found = false;
	// 			String starttime = null, starttime_seq = null, endtime = null, endtime_seq = null;
	// 	 		starttime = rs.getString(2);
	// 			starttime_seq = rs.getString(3);
	// 			endtime = rs.getString(4);
	// 			String columnValue = rs.getString(5);
	// 			String[] times = columnValue.split(";");

	// 			if(times.length > 1){
	// 				starttime = starttime + " " + times[1];
	// 				starttime_seq = starttime_seq + " " + times[2];
	// 				endtime = endtime + " " + times[3];
	// 				endtime_seq = times[0] + " " + times[4];
	// 			}
	// 			else
	// 				endtime_seq = times[0];

	// 			String[] starttimes = starttime.split(" ");
	// 			String[] starttimeSeqs = starttime_seq.split(" ");
	// 			String[] endtimes = endtime.split(" ");
	// 			String[] endtimeSeqes = endtime_seq.split(" ");

	// 			String[] starttimesRes = decode(starttimes);
 //   				String[] starttimeSeqsRes = decode(starttimeSeqs);
 //   				String[] endtimesRes = decode(endtimes);
	// 			String[] endtimeSeqesRes = decode(endtimeSeqes);

 //    			if(isInteger(starttimes[pos+1])){// if not a merged node, check time
 // 					if(Long.parseLong(endtimesRes[pos+1]) <= Long.parseLong(endTime)){
 // 						time = starttimesRes[pos+1] + "," + starttimeSeqsRes[pos+1] + "," + endtimesRes[pos+1] + "," + endtimeSeqesRes[pos+1];
 // 						String ans = rs.getString(1) + "," + time + "," + rs.getString(6) + "," + dstIdInt + "," + rs.getString(8) + "," + 
	// 						rs.getString(9) + "," + rs.getString(10) + "," + rs.getString(11) + "," + rs.getString(12) + "," + rs.getString(13);
 // 						ansList.add(ans);
 // 					}
 // 				}
 // 				else{
 // 					starttimes[pos+1] = starttimes[pos+1].replaceAll("\\(","");
 // 					starttimeSeqs[pos+1] = starttimeSeqs[pos+1].replaceAll("\\(","");
 // 					endtimes[pos+1] = endtimes[pos+1].replaceAll("\\(","");
 // 					endtimeSeqes[pos+1] = endtimeSeqes[pos+1].replaceAll("\\(","");

 // 					starttimes[pos+1] = starttimes[pos+1].replaceAll("\\)","");
 // 					starttimeSeqs[pos+1] = starttimeSeqs[pos+1].replaceAll("\\)","");
 // 					endtimes[pos+1] = endtimes[pos+1].replaceAll("\\)","");
 // 					endtimeSeqes[pos+1] = endtimeSeqes[pos+1].replaceAll("\\)","");

 // 					String[] starttimesMerged = starttimes[pos+1].split("\\.");
 // 					String[] starttimeSeqsMerged = starttimeSeqs[pos+1].split("\\.");
	// 				String[] endtimesMerged = endtimes[pos+1].split("\\.");
 // 					String[] endtimeSeqesMerged = endtimeSeqes[pos+1].split("\\.");

	// 				starttimesMerged[0] = starttimesRes[pos+1];
 // 					starttimeSeqsMerged[0] = starttimeSeqsRes[pos+1];
 // 					endtimesMerged[0] = endtimesRes[pos+1];
 // 					endtimeSeqesMerged[0] = endtimeSeqesRes[pos+1];

 // 					// long startTime = System.nanoTime();
 // 					String[] s1 = Delta.decode(starttimesMerged);
	// 				String[] s2 = Delta.decode(starttimeSeqsMerged);
	//  				String[] s3 = Delta.decode(endtimesMerged);
	//  				String[] s4 = Delta.decode(endtimeSeqesMerged);
	//  				// long endTime = System.nanoTime();
	//  				// System.out.println(endTime - startTime);
	//  				// totalExtraTime += (endTime - startTime);
	//  				for(int j = 0; j < s1.length; j++){
	//  					if(Long.parseLong(s3[j]) <= Long.parseLong(endTime)){
 // 							time = s1[j] + "," + s2[j] + "," + s3[j] + "," + s4[j];
							
 // 							String ans = rs.getString(1) + "," + time + "," + rs.getString(6) + "," + sourcIdInt + "," + rs.getString(8) + "," + 
	// 							rs.getString(9) + "," + rs.getString(10) + "," + rs.getString(11) + "," + rs.getString(12) + "," + rs.getString(13);
	//  						ansList.add(ans);
	// 					}
	// 				}
	// 		}
	// 	}
	// 	}
	// }

		//System.out.println("");
		return ansList;
 }



	public static boolean isInteger(String s){
      boolean isValidInteger = false;
      s.replaceAll("\\s+","");
      try
      {
        Long.parseLong(s);
 		isValidInteger = true;
      }
      catch(NumberFormatException ex){}
      return isValidInteger;
   }

   public static String[] decode(String[] strs){
   		String[] strsList = strs.clone();
   		
   		for(int i = 0; i < strs.length; i++){
   			if(!isInteger(strs[i])){
   				String tmp = strs[i].substring(1,strs[i].length()-1);
 				String[] tmps = tmp.split("\\.");
 				strsList[i] = tmps[0];
 			}
 			else strsList[i] = strs[i];
 		}

 		return Delta.decode(strsList);
   }
}