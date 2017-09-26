import java.sql.*;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class Main
{
  public static String user_id;
  private static String password;
  public static String dbname;
  
  public static void listFollow() 
  {
    DBConnect dbc=new DBConnect("localhost",dbname,user_id,password, "mysql");
    int cnt=0;
    try
    {
      String query = "select * from follow order by follower_id";
      ResultSet rs = dbc.searchDB(query);
      System.out.println("Follow Relations");
      System.out.println("Follower ID	Followed ID");
      while(rs.next()) 
      {
        System.out.print(rs.getString("follower_id"));
        System.out.print("	");
        System.out.println(rs.getString("followed_id"));
        cnt++;
      }
    } 
    catch(Exception e) 
    {
      System.out.println("Exception in list: follow" + e);
    } 
    finally
    {
      System.out.println(cnt);
      dbc.close();
    }
  }

  public static void addFollow(String follower_id,String followed_id)
  {
    DBConnect dbc=new DBConnect("localhost",dbname,user_id,password, "mysql");
    try
    {
      String query = String.format("select * from follow where follower_id = %s and followed_id = %s", follower_id,followed_id);
      ResultSet rs = dbc.searchDB(query);
      
      if(rs.next())
      {
        System.out.println("This relation already exisits!");
      }
      else
      {
        String insertQuery = String.format("insert into follow(follower_id,followed_id) values ('%s','%s')",follower_id,followed_id);
        dbc.updateDB(insertQuery);
      }
    }
    catch(Exception e)
    {
      System.out.println("Exception in addFollow: " + e);
    }
    finally
    {
      dbc.close();
    }
  }
  
  
  public static void updateFollow() throws FileNotFoundException, IOException
  { 
      FileReader fr= new FileReader("src/lastUpdate.txt");
      BufferedReader br= new BufferedReader(fr); 
      String line=br.readLine();
      String[] up;
      up = line.split(" ");
      String page=up[0];
      String position=up[1];
      br.close();
      fr.close();
      
      fr=new FileReader("src/dataset/links-anon_"+page+".txt");
      br=new BufferedReader(fr); 
      
      int cnt=1;
      while(true)    {
          line=br.readLine();
          
          if(line==null)    {
            br.close();
            fr.close();
            String tmpPage=Integer.toString(Integer.parseInt(page)+1);
            File f = new File("src/dataset/links-anon_"+tmpPage+".txt");
            if(!f.exists()) {
                FileWriter fw;
                fw = new FileWriter("src/lastUpdate.txt");
                BufferedWriter bw;
                bw = new BufferedWriter(fw);
                line=page+" "+Integer.toString(cnt);
                bw.write(line);
                bw.close();
                fw.close();
                return;
            }
            
            page=tmpPage;
            cnt=1;
            position="1";
            fr=new FileReader("src/dataset/links-anon_"+page+".txt");
            br=new BufferedReader(fr); 
            continue;
          }
          if(cnt>=Integer.parseInt(position))    {
              up = line.split(" ");
              String follower_id=up[0];
              String followed_id=up[1];
              addFollow(follower_id,followed_id);
          }
          cnt++;
      }
  }
  
  
  public static boolean updateFollowFast(int limit) throws FileNotFoundException, IOException
  { 
      DBConnect dbc=new DBConnect("localhost",dbname,user_id,password, "mysql");
      
      int limcnt=0;
      
      FileReader fr= new FileReader("src/lastUpdate.txt");
      BufferedReader br= new BufferedReader(fr); 
      String line=br.readLine();
      String[] up;
      up = line.split(" ");
      String page=up[0];
      String position=up[1];
      br.close();
      fr.close();
      
      fr=new FileReader("src/dataset/links-anon_"+page+".txt");
      br=new BufferedReader(fr); 
      
      int cnt=1;
      while(true)    {
          line=br.readLine();
          
          if(line==null)    {
            br.close();
            fr.close();
            String tmpPage=Integer.toString(Integer.parseInt(page)+1);
            File f = new File("src/dataset/links-anon_"+tmpPage+".txt");
            if(!f.exists()) {
                FileWriter fw;
                fw = new FileWriter("src/lastUpdate.txt");
                BufferedWriter bw;
                bw = new BufferedWriter(fw);
                line=page+" "+Integer.toString(cnt);
                bw.write(line);
                bw.close();
                fw.close();
                dbc.close();
                return false;
            }
            
            page=tmpPage;
            cnt=1;
            position="1";
            fr=new FileReader("src/dataset/links-anon_"+page+".txt");
            br=new BufferedReader(fr); 
            continue;
          }
          if(cnt>=Integer.parseInt(position))    {
              up = line.split(" ");
              String follower_id=up[0];
              String followed_id=up[1];
              
              try
              {		
                 String insertQuery = String.format("insert into follow(follower_id,followed_id) values ('%s','%s')",follower_id,followed_id);
                 dbc.updateDB(insertQuery);
                  
                  /*String query = String.format("select * from follow where follower_id = %s and followed_id = %s", follower_id,followed_id);
                  ResultSet rs = dbc.searchDB(query);

                  if(rs.next())
                  {
                    System.out.println("This relation already exists!");
                  }
                  else
                  {
                    String insertQuery = String.format("insert into follow(follower_id,followed_id) values ('%s','%s')",follower_id,followed_id);
                    dbc.updateDB(insertQuery);
                  }*/
                  
                  
                  limcnt++;
                  if(limcnt==limit)    {    
                        cnt++;
                        br.close();
                        fr.close();
                        FileWriter fw;
                        fw = new FileWriter("src/lastUpdate.txt");
                        BufferedWriter bw;
                        bw = new BufferedWriter(fw);
                        line=page+" "+Integer.toString(cnt);
                        bw.write(line);
                        bw.close();
                        fw.close();
                        dbc.close();
                        return true;
                  }
              }
              catch(Exception e)
              {
                  System.out.println("Exception in updateFollowFast: " + e);
              }
          }
          cnt++;
      }
  }
  
  public static void updateFollowFastExe() throws IOException
  {
      int blockSize=1000000;
      while(updateFollowFast(blockSize))    {}
      
  }        
  
  
  public static void biConnected(String id)
  {
    DBConnect dbc=new DBConnect("localhost",dbname,user_id,password, "mysql");
    int cnt=0;
    try
    {		
      String query = String.format("select follower_id from follow where followed_id =%s and follower_id in (select followed_id from follow where follower_id=%s) order by follower_id", id,id);
      ResultSet rs = dbc.searchDB(query);
      System.out.println("BiConnected IDs with "+id+":");
      while(rs.next()) 
      {
        System.out.println(rs.getString("follower_id"));
        cnt++;
      }
    }
    catch(Exception e)
    {
      System.out.println("Exception in biConnected: " + e);
    }
    finally
    {
      System.out.println("Total BiConnected relations for this id is " +cnt);
      dbc.close();
    }
      
  }
  
  
  public static void depthFollowerRec(String id,int depth,DBConnect dbc)
  {
    try
    {		
      String query = String.format("select node from visited where node =%s", id);
      ResultSet rs = dbc.searchDB(query);
      if(!rs.next())    {
         query = String.format("insert into visited(node) value ('%s');", id);
         dbc.updateDB(query);
         
         if(depth==0)    {
             System.out.println(id);
         }
         else    {
             query = String.format("select follower_id from follow where followed_id =%s", id);
             rs = dbc.searchDB(query);
             while(rs.next()) 
             {
                depthFollowerRec(rs.getString("follower_id"),depth-1,dbc);
             }
             
             
         }
      }
    }
    catch(Exception e)
    {
      System.out.println("Exception in depthFollowerRec: " + e);
    }
  }
  
  
  public static void depthFollower(String id,int depth)
  {
    DBConnect dbc=new DBConnect("localhost",dbname,user_id,password, "mysql");
    try
    {		
      String query = String.format("CREATE TABLE `"+dbname+"`.`visited` (`node` VARCHAR(45));");
      dbc.updateDB(query);
      System.out.println("DepthFollowerList:");
      depthFollowerRec(id,depth,dbc);
      query = String.format("DROP TABLE `"+dbname+"`.`visited`;");
      dbc.updateDB(query);
    }
    catch(Exception e)
    {
      System.out.println("Exception in depthFollower: " + e);
    }
    finally
    {
      dbc.close();
    }
      
  }
  
  
  
  
  
  public static void depthFollowedRec(String id,int depth,DBConnect dbc)
  {
    try
    {		
      String query = String.format("select node from visited where node =%s", id);
      ResultSet rs = dbc.searchDB(query);
      if(!rs.next())    {
         query = String.format("insert into visited(node) value ('%s');", id);
         dbc.updateDB(query);
         
         if(depth==0)    {
             System.out.println(id);
         }
         else    {
             query = String.format("select followed_id from follow where follower_id =%s", id);
             rs = dbc.searchDB(query);
             while(rs.next()) 
             {
                depthFollowedRec(rs.getString("followed_id"),depth-1,dbc);
             }
             
             
         }
      }
    }
    catch(Exception e)
    {
      System.out.println("Exception in depthFollowedRec: " + e);
    }
  }
  
  
  public static void depthFollowed(String id,int depth)
  {
    DBConnect dbc=new DBConnect("localhost",dbname,user_id,password, "mysql");
    try
    {		
      String query = String.format("CREATE TABLE `"+dbname+"`.`visited` (`node` VARCHAR(45));");
      dbc.updateDB(query);
      System.out.println("DepthFollowedList: ");
      depthFollowedRec(id,depth,dbc);
      query = String.format("DROP TABLE `"+dbname+"`.`visited`;");
      dbc.updateDB(query);
    }
    catch(Exception e)
    {
      System.out.println("Exception in depthFollowed: " + e);
    }
    finally
    {
      dbc.close();
    }
      
  }
  
  
  
  
  
  public static void list_follower(String x) throws IOException
  {
        DBConnect dbc=new DBConnect("localhost",dbname,user_id,password, "mysql");
        int cnt=0;
        
        try
        {
          
          String query = String.format("select follower_id from follow where followed_id=%s order by follower_id;",x);
          
          
          ResultSet rs = dbc.searchDB(query);
          
          System.out.println("List of Followers:");
          
          while(rs.next()) 
          {
            System.out.println(rs.getString("follower_id"));
            cnt++;
          }
        } 
        catch(Exception e) 
        {
          System.out.println("Exception in list_follower:" + e);
        } 
        finally
        {
          System.out.println("Total followers for this id is " +cnt);
          dbc.close();
        }
  }
  
  
  public static void list_followed(String x) throws IOException
  {
        DBConnect dbc=new DBConnect("localhost",dbname,user_id,password, "mysql");
        int cnt=0;
        
        try
        {
          String query = String.format("select followed_id from follow where follower_id=%s order by followed_id;",x);
          ResultSet rs = dbc.searchDB(query);
          
          System.out.println("List of Followeds:");
          
          while(rs.next()) 
          {
            System.out.println(rs.getString("followed_id"));
            cnt++;
          }
        } 
        catch(Exception e) 
        {
          System.out.println("Exception in list_followed:" + e);
        } 
        finally
        {
          System.out.println("Total followeds for this id is " +cnt);
          dbc.close();
        }
  }
  
  
  public static void mutual_follower(String x, String y) throws IOException
  {
        DBConnect dbc=new DBConnect("localhost",dbname,user_id,password, "mysql");
        int cnt=0;
        try
        {
          String query = String.format("select follower_id from follow where followed_id=%s and follower_id in (select follower_id from follow where followed_id=%s) order by follower_id;", x, y);
          ResultSet rs = dbc.searchDB(query);
          
          
          System.out.println("Mutual Followers:");
          
          while(rs.next()) 
          {
            System.out.println(rs.getString("follower_id"));
            cnt++;
          }
        } 
        catch(Exception e) 
        {
          System.out.println("Exception in mutual_follower:" + e);
        } 
        finally
        {
          System.out.println("Total mutual followers for this id is " +cnt);
          dbc.close();
        }
  }
  
  
  public static void mutual_followed(String x, String y) throws IOException
  {
        DBConnect dbc=new DBConnect("localhost",dbname,user_id,password, "mysql");
        int cnt=0;
        try
        {
          String query = String.format("select followed_id from follow where follower_id=%s and followed_id in (select followed_id from follow where follower_id=%s) order by followed_id;", x, y);
          ResultSet rs = dbc.searchDB(query);
          
          
          System.out.println("Mutual Followeds:");
          
          while(rs.next()) 
          {
            System.out.println(rs.getString("followed_id"));
            cnt++;
          }
        } 
        catch(Exception e) 
        {
          System.out.println("Exception in mutual_followed:" + e);
        } 
        finally
        {
          System.out.println("Total mutual followeds for this id is " +cnt);
          dbc.close();
        }
  }
  
  public static void isBiConnected(String x, String y) throws IOException
  {
        DBConnect dbc=new DBConnect("localhost",dbname,user_id,password, "mysql");
        try
        {
          String query = String.format("select * from follow where followed_id =%s and follower_id=%s and follower_id in (select followed_id from follow where follower_id=%s);", x,y,x);
          ResultSet rs = dbc.searchDB(query);
          System.out.println("BiConnecction Status:");
          
          if(rs.next()) 
          {
            System.out.println("YES");
            
          }
          else    {
              System.out.println("NO");
          }
        } 
        catch(Exception e) 
        {
          System.out.println("Exception in mutual_followed:" + e);
        } 
        finally
        {
          dbc.close();
        }
  }
  
  
  
  
  
  /*public static int socialDistanceRec(String id,String target,DBConnect dbc)
  {
    if(id.equals(target))    {
        System.out.println("gd");
        return 0;
    }
    
    try
    {		
      String query = String.format("select val from dp where node =%s", id);
      ResultSet rs = dbc.searchDB(query);
      if(!rs.next())    {
         int ret=2000000007;
         
         query = String.format("select follower_id from follow where followed_id =%s", id);
         rs = dbc.searchDB(query);
         
         while(rs.next()) 
         {
            System.out.println("good");
            int depth=1+socialDistanceRec(rs.getString("follower_id"),target,dbc);
            if(depth<ret)    ret=depth;
         }
         query = String.format("insert into dp(node,val) values ('%s','%d');",id);
         dbc.updateDB(query);
         return ret;
      }
      else    {
          int ret=rs.getInt("val");
          return ret;
      }
    }
    catch(Exception e)
    {
      System.out.println("Exception in socialDistanceRec: " + e);
      return 2000000007;
    }
      
  }
  
  
  public static int socialDistance(String id1,String id2)
  {
    DBConnect dbc=new DBConnect("localhost",dbname,user_id,password, "mysql");
    int ret=-1;
    
    try
    {		
      String query = String.format("CREATE TABLE `"+dbname+"`.`dp` (`node` VARCHAR(45),`val` INT);");
      dbc.updateDB(query);
      
      int ret1=socialDistanceRec(id1,id2,dbc,2000000007);
      query = String.format("DROP TABLE `"+dbname+"`.`dp`;");
      System.out.println("god");
      
      dbc.updateDB(query);
      query = String.format("CREATE TABLE `"+dbname+"`.`dp` (`node` VARCHAR(45),`val` INT);");
      dbc.updateDB(query);
      
      
      int ret2=socialDistanceRec(id2,id1,dbc,2000000007);
      query = String.format("DROP TABLE `"+dbname+"`.`dp`;");
      dbc.updateDB(query);
      
      if(ret1<ret2)    {
          ret=ret1;
      }
      else    {
          ret=ret2;
      }
      
      
    }
    catch(Exception e)
    {
      System.out.println("Exception in socialDistance: " + e);
      return ret;
    }
    finally
    {
      dbc.close();
    }
    return ret;
  }*/
  
  public static int socialDistanceBfs(String root,String target,DBConnect dbc) throws Exception
  {
      int serial=1;
      int head=1;
      int depth=0;
      ResultSet rs;
      String query = String.format("insert into bfs(node,ind,dep) values ('%s','%d','%d');",root,serial,depth);
      dbc.updateDB(query);
      String now=root;
      
      while(true)    {
          query = String.format("select * from bfs");
          rs= dbc.searchDB(query);
          if(!rs.next())    break;
          
          
          
          query = String.format("select * from bfs where ind=%d",head);
          rs= dbc.searchDB(query);
          
          if(rs.next())    {
              now=rs.getString("node");
              depth=rs.getInt("dep");
          }
          
          query = String.format("select follower_id from follow where followed_id =%s",now);
          rs= dbc.searchDB(query);
          
          while(rs.next()) 
            {
               String next=rs.getString("follower_id");
               if(next.equals(target))    {
                   return depth+1;
               }
               
               query = String.format("select node from visited where node =%s", next);
               ResultSet rs2 = dbc.searchDB(query);
              
               if(rs2.next())    continue;
               
               serial++;
               query = String.format("insert into bfs(node,ind,dep) values ('%s','%d','%d');",next,serial,depth+1);
               dbc.updateDB(query);
               query = String.format("insert into visited(node) values ('%s');", next);
               dbc.updateDB(query);

            }
            query=String.format("delete from bfs where ind=%d",head);
            dbc.updateDB(query);
            head++;
      }
      
      
      
      return 2000000007;
      
  }
  
  
  public static int socialDistance(String id1,String id2)
  {
    DBConnect dbc=new DBConnect("localhost",dbname,user_id,password, "mysql");
    int ret=2000000007;
    
    try
    {		
      String query = String.format("SHOW TABLES LIKE `"+dbname+"`.`bfs`;");
      ResultSet rs = dbc.searchDB(query);
      
      if(rs.next())    {
           query = String.format("DROP TABLE `"+dbname+"`.`bfs`;");
            dbc.updateDB(query);
      }
      query = String.format("SHOW TABLES LIKE `"+dbname+"`.`visited`;");
      rs = dbc.searchDB(query);
      
      if(rs.next())    {
           query = String.format("DROP TABLE `"+dbname+"`.`visited`;");
            dbc.updateDB(query);
      }
      
      
        
      
      query = String.format("CREATE TABLE `"+dbname+"`.`bfs` (`node` VARCHAR(45),`ind` INT,`dep` INT);");
      dbc.updateDB(query);
      query = String.format("CREATE TABLE `"+dbname+"`.`visited` (`node` VARCHAR(45));");
      dbc.updateDB(query);
      
      int ret1=socialDistanceBfs(id1,id2,dbc);
      query = String.format("DROP TABLE `"+dbname+"`.`bfs`;");
      dbc.updateDB(query);
      query = String.format("DROP TABLE `"+dbname+"`.`visited`;");
      dbc.updateDB(query);
      
      
      query = String.format("CREATE TABLE `"+dbname+"`.`bfs` (`node` VARCHAR(45),`ind` INT,`dep` INT);");
      dbc.updateDB(query);
      query = String.format("CREATE TABLE `"+dbname+"`.`visited` (`node` VARCHAR(45));");
      dbc.updateDB(query);
      
      
      int ret2=socialDistanceBfs(id2,id1,dbc);
      query = String.format("DROP TABLE `"+dbname+"`.`bfs`;");
      dbc.updateDB(query);
      query = String.format("DROP TABLE `"+dbname+"`.`visited`;");
      dbc.updateDB(query);
      
      if(ret1<ret2)    {
          ret=ret1;
      }
      else    {
          ret=ret2;
      }
      
    }
    catch(Exception e)
    {
      System.out.println("Exception in socialDistance: " + e);
      return ret;
    }
    finally
    {
      dbc.close();
    }
    return ret;
  }
  
  
  
  
  
  

  public static void main(String args[]) throws IOException
  {
      
    Scanner scanner;
    scanner = new Scanner(System.in);
    while(true)    {
        System.out.println("Enter Username: ");
        user_id=scanner.next();
        System.out.println("Enter Password: ");
        password=scanner.next(); 
        dbname="twitter";
        if("root".equals(user_id) && "1953".equals(password))    {
            break;
        }
        System.out.println("Wrong Id or Pass");
    }
    
    //updateFollowFast(400000);
    //updateFollow();
    //updateFollowFastExe();
    //listFollow();
    
    System.out.println("Welcome to Twitter Follower-Followed data analysis!");
    
    while(true)
    {
        
      System.out.println("Option 1: Single user analysis\nOption 2: Pair user analysis\nOption 3: Insert data\nOption 4: Show Relations");
      System.out.println("Enter your option: ");
      
      int opt= scanner.nextInt();
      
      
      if(opt==1)    {
          while(true)    {
              System.out.println("Enter id: ");
              String id;
              id=scanner.next();
              System.out.println("Query 1: List of Followers \n"
                           + "Query 2: List of Followeds \n"
                           + "Query 3: List of Bi-Connecteds \n"
                           + "Query 4: Depth followeds \n"
                           + "Query 5: Depth followers");
          
                opt= scanner.nextInt();
                
                if(opt==1)    {
                    list_follower(id);
                }

                else if(opt==2)    {
                    list_followed(id);
                }

                else if(opt==3)    {
                    biConnected(id);
                }

                else if(opt==4)    {
                     System.out.println("Input depth:");
                     int depth;
                     depth=scanner.nextInt();
                     depthFollowed(id,depth);
                }

                else if(opt==5)    {
                     System.out.println("Input depth:");
                     int depth;
                     depth=scanner.nextInt();
                     depthFollower(id,depth); 
                }

                else    {
                    System.out.println("Wrong Query");
                }
                System.out.println("Do you want to continue option 1 (y/n)?");
                String choice2 = scanner.next();			
                if (choice2.equalsIgnoreCase("n")) break;
          }
          
          
          
          
          
          
      }
      else if(opt==2) {
           while(true)    {
              String id1,id2;
              System.out.println("Enter id1: ");
              id1=scanner.next();
              System.out.println("Enter id2: ");
              id2=scanner.next();
              
              System.out.println("Query 1: List of mutual Followers \n"
                           + "Query 2: List of mutual Followeds \n"
                           + "Query 3: Bi-Connected check \n"
                           + "Query 4: Social Distance");
          
                opt= scanner.nextInt();
                if(opt==1)    {
                    mutual_follower(id1,id2);
                }

                else if(opt==2)    {
                    mutual_followed(id1,id2);
                }

                else if(opt==3)    {
                    isBiConnected(id1,id2);
                }

                else if(opt==4)    {
                    int distance=socialDistance(id1,id2);
                    System.out.println("Social distance between this pair is:"+distance);
                }

                else    {
                    System.out.println("Wrong Query");
                }
                System.out.println("Do you want to continue option 2 (y/n)?");
                String choice2 = scanner.next();			
                if (choice2.equalsIgnoreCase("n")) break;
           }
      }
      
      else if(opt==3)    {
              System.out.println("Insert Follower_id:");
              String id4=scanner.next();
              System.out.println("Insert Followed_id:");
              String id5=scanner.next();
              addFollow(id4,id5);
      }
      
      else if(opt==4)    {
          listFollow();
      }
      
      else    {
          System.out.println("Wrong Options");
      }
      
      System.out.println("Do you want to continue querying at all(y/n)?");
      String choice = scanner.next();			
      if (choice.equalsIgnoreCase("n")) break;
    }
    scanner.close();
  }
}

