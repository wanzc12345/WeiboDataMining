import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import weibo4j.Friendships;
import weibo4j.Timeline;
import weibo4j.Users;
import weibo4j.Weibo;
import weibo4j.model.Paging;
import weibo4j.model.Status;
import weibo4j.model.StatusWapper;
import weibo4j.model.User;
import weibo4j.model.UserWapper;
import weibo4j.model.WeiboException;

public class FetchData {
	
	String access_token;
	Weibo weibo;
	Friendships friendships;
	String[] ids;

	public FetchData() throws WeiboException, IOException{
		
		//get access token from command line
		System.out.println("Enter the access token:");
		access_token = new Scanner(System.in).nextLine();
		
		//initialization
		weibo = new Weibo();
		weibo.setToken(access_token);
		friendships = new Friendships();
		
		System.out.println("Enter your screenname of weibo:\n");
		String screenName = new Scanner(System.in).nextLine();
		
		System.out.println("Enter your uid of weibo:\n");
		String userId = new Scanner(System.in).nextLine();
		
		System.out.println("Which do you want to recommend for?\n1. Friends\n2. Followers\n");
		int isFollowers = Integer.valueOf(new Scanner(System.in).nextLine());
		
		System.out.println("Enter the amount of users you want to recommend for:");
		int numberOfFriends = Integer.valueOf(new Scanner(System.in).nextLine());
		
		//get users
//		ids = friendships.getFollowersIdsByName("万志程",100,0);
		
		UserWapper uw;
		if(isFollowers == 1)
			uw = friendships.getFollowersByName(screenName, numberOfFriends, 0);
		else
			uw = friendships.getFriendsBilateral(userId, 0, new Paging(1, numberOfFriends));
		
		Jedis jedis = new Jedis("127.0.0.1");
		
		//fetch history data
		FetchHistoryData();
		
		FileOutputStream fos = new FileOutputStream("result.txt",true);
		fos.write(("*"+"178518893"+":"+"万志程"+"**\n").getBytes());
		//fos.close();
		SaveBlogsToRedisByUid("1785181893");
		
		//save all users
		//save microblogs by id
		for(User u:uw.getUsers()){
			
			if(jedis.zscore("users", u.getId())==null){
					jedis.zadd("users", jedis.zcard("users"), u.getId());
					jedis.zadd("names", jedis.zcard("names"), u.getName());
					fos.write(("*"+u.getId()+":"+u.getName()+"**\n").getBytes());
					//fos.close();
			}
					
			//jedis.del(u.getId());
			
			//System.out.println("**********************UID: "+u.getId()+":"+u.getName()+"**************************\n");
			
			SaveBlogsToRedisByUid(u.getId().trim());
			
		}
		
		Set<Tuple> tuples = jedis.zrangeWithScores("items",0,10000);
		for(Tuple t:tuples){
			
			fos.write((t.getScore()+" "+t.getElement()+"\n").getBytes());
			//fos.close();
			
			//System.out.println(t.getScore()+" "+t.getElement());
		}
		fos.close();
		
//		Jedis jedis = new Jedis("127.0.0.1");
//		jedis.del("2799737155");
//		SaveBlogsToRedisByUid("2799737155");
		
	}
	
	private boolean FetchHistoryData() throws FileNotFoundException, IOException{
		
		Jedis jedis = new Jedis("127.0.0.1");
		
		//clean the database
		jedis.del("users");
		jedis.del("names");
		jedis.del("items");
		
		if((new File("result.txt")).exists()){
		//read history file
		FileInputStream fis = new FileInputStream("result.txt");
		BufferedReader buffer = new BufferedReader(new InputStreamReader(fis));
		String line, userId = "1785181893", userName;
		
		while((line = buffer.readLine())!=null){
			
			System.out.println(line);
			
			if(line.startsWith("*")){
				
				userId = line.substring(line.indexOf("*")+1, line.indexOf(":"));
				userName = line.substring(line.indexOf(":")+1, line.indexOf("**"));
				jedis.del(userId);
				jedis.zadd("users", jedis.zcard("users"),userId);
				jedis.zadd("names", jedis.zcard("names"),userId);
				System.out.println(userId+":"+userName);
				
			}else{
				
				String score = line.substring(0, line.indexOf(" "));
				String item = line.substring(line.indexOf(" ")+1);
				jedis.zadd(userId, Double.valueOf(score), item);
				if(jedis.zscore("items", item)==null){
					jedis.zadd("items", jedis.zcard("items"), item);
				}
				System.out.println(score+":"+item);
				
			}
		}
		}
		
		return true;
		
	}
	
	private boolean SaveBlogsToRedisByUid(String uid) throws WeiboException, IOException{
		
		Users users = new Users();
		users.showUserById(uid);
		
		//RT
		StatusWapper statuses = new Timeline().getUserTimelineByUid(uid,new Paging(1,50),0,0);
		
		Jedis jedis = new Jedis("127.0.0.1");
		
		for(Status s:statuses.getStatuses()){
			
			/*if(uid.equals("1784667055")){
				System.out.println("what the hell:"+uid+s.getText());
				System.in.read();
			}else
				return true;*/
			
			//treat the case of retweeted and original
			if(s.getRetweetedStatus()!=null&&s.getRetweetedStatus().getUser()!=null){
				
				String [] wtf = Retrieval(s.getText()+" "+"@"+s.getRetweetedStatus().getUser().getScreenName()
						+":"+s.getRetweetedStatus().getText()).split(";");
				
//				System.out.print(jedis.zscore(uid, wtf[1])+"*******************************");
				
				for(String str:wtf){
					
					if(str.length()!=0){
					if(jedis.zscore(uid, str)!=null)
						jedis.zadd(uid, jedis.zscore(uid, str)+1, str);
					else
						jedis.zadd(uid, 1, str);
					
					if(jedis.zscore("items", str)==null)
						jedis.zadd("items", jedis.zcard("items"), str);
					}
				}
						
//				jedis.sadd(uid, Retrieval(s.getText()+";"+"@"+s.getRetweetedStatus().getUser().getScreenName()
//						+":"+s.getRetweetedStatus().getText()).split(";"));
			
				
				
				//System.out.println(wtf);
				
			}else{
				
//				jedis.sadd(uid, Retrieval(s.getText()+"\n").split(";"));
				
				String [] wtf = Retrieval(s.getText()+"\n").split(";");
				
				for(String str:wtf){
					if(str.length()!=0){
					if(jedis.zscore(uid, str)!=null)
						jedis.zadd(uid, jedis.zscore(uid, str)+1, str);
					else
						jedis.zadd(uid, 1, str);
					
					if(jedis.zscore("items", str)==null)
						jedis.zadd("items", jedis.zcard("items"), str);
					}
				}
			
				//System.out.println(wtf);
				
			}
			
		}
		
		Set<Tuple> tuples = jedis.zrangeWithScores(uid, 0, 1000);
		for(Tuple t:tuples){
			
			if(t.getScore()!=0){
				FileOutputStream fos = new FileOutputStream("result.txt",true);
				fos.write((t.getScore()+" "+t.getElement()+"\n").getBytes());
				fos.close();
			}
			
			//System.out.println(t.getScore()+" "+t.getElement());
		}
		
//		Set<String> fuck = jedis.smembers("1987304775");
//		System.out.println("Start:");
//		for(String s:fuck)
//			System.out.print(s);
		
		return true;
		
	}
	
	private String  Retrieval(String content){
		
		String result="";
		String temp=content;
		
		//retrieval name after '@'
		while(temp.contains("@")){
			
			temp = temp.substring(temp.indexOf("@")+1);
			
			int [] distances = {temp.indexOf(":"),temp.indexOf("："),temp.indexOf("）"),temp.indexOf(" ")
								,temp.indexOf(";"),temp.indexOf("，"),temp.indexOf("["),temp.indexOf("@")};
			
//			for(int i:distances)
//				System.out.println(i);
//			
//			System.out.println();
			
			int nearest = min(distances);
			
			if(temp.indexOf(":")==nearest)
				result += temp.substring(0,temp.indexOf(":")).trim()+";";
			else if(temp.indexOf("：")==nearest)
				result += temp.substring(0,temp.indexOf("：")).trim()+";";
			else if(temp.indexOf(")")==nearest)
				result += temp.substring(0,temp.indexOf(")")).trim()+";";
			else if(temp.indexOf("）")==nearest)
				result += temp.substring(0,temp.indexOf("）")).trim()+";";
			else if(temp.indexOf(" ")==nearest)
				result += temp.substring(0,temp.indexOf(" ")).trim()+";";
			else if(temp.indexOf("[")==nearest)
				result += temp.substring(0,temp.indexOf("[")).trim()+";";
			else if(temp.indexOf(";")==nearest)
				result += temp.substring(0,temp.indexOf(";")).trim()+";";
			else if(temp.indexOf("，")==nearest)
				result += temp.substring(0,temp.indexOf("，")).trim()+";";
			else if(temp.indexOf("@")==nearest)
				result += temp.substring(0,temp.indexOf("@")).trim()+";";
			else
				result += temp.trim()+";";
			
		}
		
		temp=content;
		
		//retrieval topic
		while(temp.contains("#")){
			
			temp = temp.substring(temp.indexOf("#")+1);
			if(temp.contains("#")){
				result += temp.substring(0,temp.indexOf("#")).trim()+";";
				temp = temp.substring(temp.indexOf("#")+1);
			}
			
		}
		
		temp=content;
		
		//retrieval topic
		while(temp.contains("【")){
			
			temp = temp.substring(temp.indexOf("【")+1);
			if(temp.contains("】")){
				result += temp.substring(0,temp.indexOf("】")).trim()+";";
				temp = temp.substring(temp.indexOf("】")+1);
			}
			
		}
		
//		temp=content;
//		
//		//retrieval topic
//		while(temp.contains("\"")){
//			
//			temp = temp.substring(temp.indexOf("\"")+1);
//			if(temp.contains("\"")){
//				result += temp.substring(0,temp.indexOf("\"")).trim()+";";
//				temp = temp.substring(temp.indexOf("\"")+1);
//			}
//			
//		}
		
		temp=content;
		
		//retrieval topic
		while(temp.contains("\'")){
			
			temp = temp.substring(temp.indexOf("\'")+1);
			if(temp.contains("\'")){
				result += temp.substring(0,temp.indexOf("\'")).trim()+";";
				temp = temp.substring(temp.indexOf("\'")+1);
			}
			
		}
		
		temp=content;
		
		//retrieval url
		while(temp.contains("http://t.cn/")){
			
			temp = temp.substring(temp.indexOf("http://t.cn/")+12);
			int i=0;
			for(;i<temp.length();i++)
				if((temp.codePointAt(i)>90&&temp.codePointAt(i)<97)
						||temp.codePointAt(i)>122
						||temp.codePointAt(i)<48
						||(temp.codePointAt(i)>57&&temp.codePointAt(i)<65))
					break;
			
			result += "http://t.cn/"+temp.substring(0, i).trim()+";";
			
//			if(temp.contains(" "))
//				result += "http://t.cn/"+temp.substring(0,temp.indexOf(" ")).trim()+";";
//			else if(temp.contains(" "))
//				result += "http://t.cn/"+temp.substring(0,temp.indexOf(" ")).trim()+";";
//			else
//				result += "http://t.cn/"+temp.trim()+";";
			
		}

		return result;
	}
	
	//return the minimum value of distances
	private int min(int[] distances){

		int min=100;
		
		for(int i:distances){
			if(i>=0)
				min = (min<=i?min:i);
		}
		return min;
		
	}
	
	public static void main(String[] argv) throws WeiboException, IOException, InterruptedException{

		while(true){
			
			new FetchData();

			System.out.println("Wait for an hour to refresh......");
			Thread.sleep(3600);
		
		}
//		System.out.println(jedis.get("first"));
	}
}
