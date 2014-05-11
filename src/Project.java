import java.sql.*;
import java.util.Vector;
import java.util.HashMap;


public class Project {

	public static void main(String[] args) {
		// set up database's user name, password and URL
		String usr = "postgres";
		String pwd = "gm76719686";
		String url = "jdbc:postgresql://localhost:5432/postgres";
		try { 
			Class.forName("org.postgresql.Driver");
			System.out.println("Success loading Driver!");
		} catch (Exception e) {
			System.out.println("Fail loading Driver!");
			e.printStackTrace();
		}
		Project pro = new Project();

		HashMap<String,Value> class1 =  new HashMap<String,Value>();

		try {
			Connection conn1 = DriverManager.getConnection(url, usr, pwd);
			System.out.println("Success connecting server!");
			// get query result to ResultSet rs
			Statement stmt1 = conn1.createStatement();
			ResultSet rs1 = stmt1.executeQuery("SELECT * FROM Sales");

			Vector<String> checkboard1 = new Vector<String>();
			while (rs1.next()){	
				//create a new Data class object to store the needed information
				String cust=rs1.getString("cust");
				String prod=rs1.getString("prod");
				int day=rs1.getInt("day");
				int month=rs1.getInt("month");
				int year=rs1.getInt("year");
				String state=rs1.getString("state");
				int quant=rs1.getInt("quant");
				String comb = cust+"_"+prod;

				if (checkboard1.contains(comb)){
					int index = checkboard1.indexOf(comb);
					if (state.equals("NY")&&year==(1990)){
						class1.get(comb).sum += quant; 
						class1.get(comb).count++; 
						class1.get(comb).avg = class1.get(comb).sum/class1.get(comb).count; 
						if (class1.get(comb).max < quant){
							class1.get(comb).max = quant;
						}
						if (class1.get(comb).min > quant){
							class1.get(comb).min = quant;
						}
					}
				}
				else {
					if (state.equals("NY")&&year==(1990)){
						Value value = pro.new Value();
						value.sum += quant; 
						value.count++; 
						value.min = quant; 
						value.max = quant; 
						value.avg = quant; 
						checkboard1.addElement(comb);
						class1.put(comb,value);
					}
					else {
						Value value2 = pro.new Value();
						value2.sum =0; 
						value2.count = 0; 
						value2.min = 0; 
						value2.max = 0; 
						value2.avg = 0; 
						checkboard1.addElement(comb);
						class1.put(comb,value2);
					}
 				}

			}			// close the connection to the server.
			conn1.close();
			stmt1.close();
			rs1.close();
		}				
		// catch the exception if it failed to connected the server.
		catch (SQLException e) {
			System.out.println("Connection URL or username or password errors!");
			e.printStackTrace();
		}

		HashMap<String,Value> class2 =  new HashMap<String,Value>();

		try {
			Connection conn2 = DriverManager.getConnection(url, usr, pwd);
			System.out.println("Success connecting server!");
			// get query result to ResultSet rs
			Statement stmt2 = conn2.createStatement();
			ResultSet rs2 = stmt2.executeQuery("SELECT * FROM Sales");

			Vector<String> checkboard2 = new Vector<String>();
			while (rs2.next()){	
				//create a new Data class object to store the needed information
				String cust=rs2.getString("cust");
				String prod=rs2.getString("prod");
				int day=rs2.getInt("day");
				int month=rs2.getInt("month");
				int year=rs2.getInt("year");
				String state=rs2.getString("state");
				int quant=rs2.getInt("quant");
				String comb = cust+"_"+prod;

				if (checkboard2.contains(comb)){
					int index = checkboard2.indexOf(comb);
					if (state.equals("NJ")&&year==(1990)){
						class2.get(comb).sum += quant; 
						class2.get(comb).count++; 
						class2.get(comb).avg = class2.get(comb).sum/class2.get(comb).count; 
						if (class2.get(comb).max < quant){
							class2.get(comb).max = quant;
						}
						if (class2.get(comb).min > quant){
							class2.get(comb).min = quant;
						}
					}
				}
				else {
					if (state.equals("NJ")&&year==(1990)){
						Value value = pro.new Value();
						value.sum += quant; 
						value.count++; 
						value.min = quant; 
						value.max = quant; 
						value.avg = quant; 
						checkboard2.addElement(comb);
						class2.put(comb,value);
					}
					else {
						Value value2 = pro.new Value();
						value2.sum =0; 
						value2.count = 0; 
						value2.min = 0; 
						value2.max = 0; 
						value2.avg = 0; 
						checkboard2.addElement(comb);
						class2.put(comb,value2);
					}
 				}

			}			// close the connection to the server.
			conn2.close();
			stmt2.close();
			rs2.close();
		}				
		// catch the exception if it failed to connected the server.
		catch (SQLException e) {
			System.out.println("Connection URL or username or password errors!");
			e.printStackTrace();
		}

		HashMap<String,Value> class3 =  new HashMap<String,Value>();

		try {
			Connection conn3 = DriverManager.getConnection(url, usr, pwd);
			System.out.println("Success connecting server!");
			// get query result to ResultSet rs
			Statement stmt3 = conn3.createStatement();
			ResultSet rs3 = stmt3.executeQuery("SELECT * FROM Sales");

			Vector<String> checkboard3 = new Vector<String>();
			while (rs3.next()){	
				//create a new Data class object to store the needed information
				String cust=rs3.getString("cust");
				String prod=rs3.getString("prod");
				int day=rs3.getInt("day");
				int month=rs3.getInt("month");
				int year=rs3.getInt("year");
				String state=rs3.getString("state");
				int quant=rs3.getInt("quant");
				String comb = cust+"_"+prod;

				if (checkboard3.contains(comb)){
					int index = checkboard3.indexOf(comb);
					if (state.equals("CT")&&year==(1990)){
						class3.get(comb).sum += quant; 
						class3.get(comb).count++; 
						class3.get(comb).avg = class3.get(comb).sum/class3.get(comb).count; 
						if (class3.get(comb).max < quant){
							class3.get(comb).max = quant;
						}
						if (class3.get(comb).min > quant){
							class3.get(comb).min = quant;
						}
					}
				}
				else {
					if (state.equals("CT")&&year==(1990)){
						Value value = pro.new Value();
						value.sum += quant; 
						value.count++; 
						value.min = quant; 
						value.max = quant; 
						value.avg = quant; 
						checkboard3.addElement(comb);
						class3.put(comb,value);
					}
					else {
						Value value2 = pro.new Value();
						value2.sum =0; 
						value2.count = 0; 
						value2.min = 0; 
						value2.max = 0; 
						value2.avg = 0; 
						checkboard3.addElement(comb);
						class3.put(comb,value2);
					}
 				}

			}			// close the connection to the server.
			conn3.close();
			stmt3.close();
			rs3.close();
		}				
		// catch the exception if it failed to connected the server.
		catch (SQLException e) {
			System.out.println("Connection URL or username or password errors!");
			e.printStackTrace();
		}

		//Print the output.
		System.out.println("CUSTOMER PRODUCT 1_SUM_QUANT 2_SUM_QUANT 3_SUM_QUANT ");
		System.out.println("======== ======= =========== =========== =========== ");
		for(String key : class1.keySet()){
			System.out.print(key.split("_")[0] + pro.repeat_char(" ",9 - key.split("_")[0].length()));
			System.out.print(key.split("_")[1] + pro.repeat_char(" ",8 - key.split("_")[1].length()));
			System.out.print(pro.repeat_char(" ", 9 - String.valueOf(class1.get(key).sum).length()) + class1.get(key).sum + " "); 
			System.out.print(pro.repeat_char(" ", 9 - String.valueOf(class2.get(key).sum).length()) + class2.get(key).sum + " "); 
			System.out.print(pro.repeat_char(" ", 9 - String.valueOf(class3.get(key).sum).length()) + class3.get(key).sum + " "); 
			System.out.println();
		}
	}

	public String repeat_char(String ch, int count){
		String result = "";
		for(int j = 0;j<count;j++){
			result = result + ch;
		}
		return result;
	};

	class mf_structure_ga{
		public String cust = "";
		public String prod = "";
	}
	class Value{
		public long sum = 0;
		public long avg = 0;
		public long max = 0;
		public long min = 0;
		public int count = 0;
	}

}
