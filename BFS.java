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
public class BFS{
	public static void main(String[] args){
		String dbname = args[4], dbAddress = args[5], user = args[0], origdbName = args[7],
		passwd = args[1],dstId = args[2], endTimeStr = args[3], type = args[6]; // Set up database info
		boolean newquery = false;
		Map<Integer,Long> visited = new HashMap();
		Map<Integer, Set<Integer>> nodes2Newnode = new HashMap();
		Map<Integer, Set<Integer>> newnode2node = new HashMap();
		Long timeM = 0L, seqM = 0L;
		//System.out.println(dbAddress + '\n');
    	if(type.equals("compress")){
      		newquery = true;
		try{
			BufferedReader reader = new BufferedReader(new FileReader(origdbName+"data/nodes.csv"));
			String line = reader.readLine();
			String[] ms = line.split(",");
			timeM = Long.parseLong(ms[0]);
			seqM = Long.parseLong(ms[1]);
			
			while(line != null){
				line = reader.readLine();
				String[] strs = line.split(",");
				int newNode = Integer.parseInt(strs[0]);
				Set<Integer> nodes = new HashSet();
				for(int i = 1; i < strs.length; i++){
					int node = Integer.parseInt(strs[i]);
					nodes.add(node);
					// Set nodeSet = nodes2Newnode.getOrDefault(node,new HashSet());
					// nodeSet.add(newNode);
					// nodes2Newnode.put(node, newNode);
				}
				newnode2node.put(newNode, nodes);
			}
		}
		catch(Exception exp){}
		}
		Query query = new Query();
		query.setNodesMapping(nodes2Newnode,newnode2node);
		long startTime = 0L;
		try{
			String path = origdbName+"data/queryResult" + dbname + ".csv";
			BufferedWriter writer = new BufferedWriter(new FileWriter(path)); 
			Connection conn = PostgreSQLJDBC.connDB(dbAddress,user,passwd);
			Statement statement = conn.createStatement();
			Scanner myObj = new Scanner(System.in);  // Create a Scanner object
			System.out.println("dst id is: " + dstId); 
    		System.out.println("time is: " + endTimeStr); 
    		startTime = System.nanoTime();
    		Queue<String> queue = new LinkedList<>();
    		ArrayList<String> queryList;
    		if(newquery)
    			queryList = query.singleQuery(endTimeStr,dstId, statement);
    		else
				queryList = query.originQuery(endTimeStr,dstId, statement);
			if(queryList.isEmpty()){
				System.out.println("no Result");
				return;
			}
			for(int i = 0; i < queryList.size(); i ++)
				queue.offer(queryList.get(i));
			System.out.println("Doing BFS...");
			while(!queue.isEmpty()){
				String queryStr = queue.poll();
				writer.write(queryStr);
				writer.newLine();  
				String[] queryStrs = queryStr.split(",");
				if(visited.keySet().contains(Integer.parseInt(queryStrs[1]))){
					if(Long.parseLong(queryStrs[0]) <= visited.get(Integer.parseInt(queryStrs[1])))
						continue;
					else
						visited.remove(Integer.parseInt(queryStrs[1]));
				}
				endTimeStr = queryStrs[0];
				dstId = queryStrs[1];
				visited.put(Integer.parseInt(dstId),Long.parseLong(endTimeStr));
                                                                                     
				if(newquery)
    				queryList = query.singleQuery(endTimeStr,dstId, statement);
    			else
					queryList = query.originQuery(endTimeStr,dstId, statement);

				for(int i = 0; i < queryList.size(); i ++)
					queue.offer(queryList.get(i));
			}
			writer.close(); 
		}
		catch(Exception exp){
			exp.printStackTrace(); 
       		System.out.println(exp); 
		}
		String nodepath = origdbName+"data/nodeResult" + dbname;
		try{
			BufferedWriter writer = new BufferedWriter(new FileWriter(nodepath)); 
			writer = new BufferedWriter(new FileWriter(nodepath)); 
			Iterator<Integer> itr = visited.keySet().iterator();
			while(itr.hasNext()){
 				writer.write(String.valueOf(itr.next()));
 				writer.write(",");
			}
			writer.close();
		}catch(Exception exp){
			exp.printStackTrace(); 
       		System.out.println(exp); 
		}

		System.out.println("totalNodes:" + visited.size());
		long endTime  = System.nanoTime();
		double totalTime = (endTime - startTime)/1000000000.0;
		System.out.println("running time is:" + totalTime);
		 System.out.println("Meg used="+(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())/(1000*1000)+"M");
		System.out.println("------");
		
	}
}