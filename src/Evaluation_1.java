import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Vector;

public class Evaluation_1 {
	
	//aggregate function and condition check table(map)
	private static Vector<String> aggs;
	private static HashMap<String,Vector<String>> log_op;
	public static int count = 0;
	private static HashMap<String, Vector<String>> af_check;
	private static HashMap<String, Vector<String>> condition_check;
	
	private static BufferedReader br;

	// six operands
	public static String[] atts; //For selected attributes
	public static int Num_GV;
	public static String[] grouping_atts;
	public static String[] agg_funcs;
	public static Vector<String> predicates;
	
	//class name
	public static String GA_class = "mf_structure_ga";
	public static String AF_class = "mf_structure_af";
	
	//class contents
	public static String imports = 
			"import java.sql.*;\n" +
			"import java.util.Vector;\n"+
			"import java.util.HashMap;\n";
	
	public static String class_name = "\npublic class Project {\n";
	public static String class_name_tail = "}";
	
	public static String class_main_front = 
			"	public static void main(String[] args) {\n"
			+ "		// set up database's user name, password and URL\n"
			+ "		String usr = \"postgres\";\n" 
			+ "		String pwd = \"gm76719686\";\n"
			+ "		String url = \"jdbc:postgresql://localhost:5432/postgres\";\n"
			+ "		try { \n" 
			+ "			Class.forName(\"org.postgresql.Driver\");\n"
			+ "			System.out.println(\"Success loading Driver!\");\n"
			+ "		} catch (Exception e) {\n"
			+ "			System.out.println(\"Fail loading Driver!\");\n"
			+ "			e.printStackTrace();\n" 
			+ "		}\n"
			+ "		Project pro = new Project();\n";
	
	public static String class_main_tail = 
			"	}\n";
	
	// the nth grouping variable 
	public static String GV_num;
	
	// Main functions
	public static void main(String[] args) {
		
		// read all the operands from the file.
		get_S_Attributes();
		get_G_Number();
		get_G_Attributes();
		get_F_Aggregates();
		get_Conditions();

		generate_class();

	}
	
	//get connection front String
	public static String get_connection_front_String(String GV_num){
		String connection_front = 
				"		try {\n"
				+ "			Connection conn" + GV_num + " = DriverManager.getConnection(url, usr, pwd);\n"
				+ "			System.out.println(\"Success connecting server!\");\n"
				+ "			// get query result to ResultSet rs\n"
				+ "			Statement stmt" + GV_num + " = conn" + GV_num + ".createStatement();\n"
				+ "			ResultSet rs" + GV_num + " = stmt" + GV_num + ".executeQuery(\"SELECT * FROM Sales\");\n";
		return connection_front;
	}
	
	//get connection tail String
	public static String get_connection_tail_String(String GV_num){
		String connection_tail = 
				"			}"			
				+ "			// close the connection to the server.\n" 
				+ "			conn" + GV_num + ".close();\n"
				+ "			stmt" + GV_num + ".close();\n" 
				+ "			rs" + GV_num + ".close();\n" 
				+ "		}				\n"
				+ "		// catch the exception if it failed to connected the server.\n"
				+ "		catch (SQLException e) {\n"
				+ "			System.out.println(\"Connection URL or username or password errors!\");\n"
				+ "			e.printStackTrace();\n" 
				+ "		}\n";
		return connection_tail;
	}
	
	//produce the value class for each grouping variable
	public static String produce_value_class(String GV_num){
		String classes ="		HashMap<String,Value> class" + GV_num + " " + "=  new HashMap<String,Value>();\n";		
		return classes;
	}
	
	//read the data line by line from disk
	public static String retrieve_data(String GV_num){
		String data = 
				"			Vector<String> checkboard" + GV_num + " = new Vector<String>();\n" + 
				"			while (rs" + GV_num + ".next()){	\n"
				+ "				//create a new Data class object to store the needed information\n"
				+ "				String cust=rs" + GV_num + ".getString(\"cust\");\n"
				+ "				String prod=rs" + GV_num + ".getString(\"prod\");\n"
				+ "				int day=rs" + GV_num + ".getInt(\"day\");\n"
				+ "				int month=rs" + GV_num + ".getInt(\"month\");\n"
				+ "				int year=rs" + GV_num + ".getInt(\"year\");\n"
				+ "				String state=rs" + GV_num + ".getString(\"state\");\n"
				+ "				int quant=rs" + GV_num + ".getInt(\"quant\");\n"
				+ "				String comb = " +  grouping_att()+ "\n"
				+ ""			;

		return data;
	}
	
	public static String grouping_att(){
		String t ="";
		for (String a:grouping_atts){
			t += "+" + "\"_\""  + "+" + a;
		}
		return t.substring(5)+ ";";
	}

	//process the data retrieved from each line and update the HashMap used to store information.
	public static String checkboard_operation(String GV_num){
		String operation = 
				"				if (checkboard" + GV_num + ".contains(comb)){\n" 
				+ "					int index = " + "checkboard" + GV_num + ".indexOf(comb);\n"
				+ "					if (" + get_conditions(GV_num) + "){\n"
				+ "						class" + GV_num + ".get(comb).sum += quant; \n"	
				+ "						class" + GV_num + ".get(comb).count++; \n"
				+ "						class" + GV_num + ".get(comb).avg = class" + GV_num + ".get(comb).sum/class" + GV_num + ".get(comb).count; \n"
				+ "						if (class" + GV_num + ".get(comb).max < quant){\n"
				+ "							class" + GV_num + ".get(comb).max = quant;\n"
				+ "						}\n"
				+ "						if (class" + GV_num + ".get(comb).min > quant){\n"
				+ "							class" + GV_num + ".get(comb).min = quant;\n"
				+ "						}\n"
				+ "					}\n"				
				+ "				}\n"
				+ "				else {\n" 
				+ "					if (" + get_conditions(GV_num) + "){\n"
				+ "						Value value = pro.new Value();\n"
				+ "						value.sum += quant; \n"	
				+ "						value.count++; \n"	
				+ "						value.min = quant; \n"
				+ "						value.max = quant; \n"
				+ "						value.avg = quant; \n"
				+ "						checkboard" + GV_num + ".addElement(comb);\n"	
				+ "						class" + GV_num + ".put(comb,value);\n"
				+ "					}\n"
				+ "					else {\n"
				+ "						Value value2 = pro.new Value();\n"
				+ "						value2.sum =0; \n"	
				+ "						value2.count = 0; \n"	
				+ "						value2.min = 0; \n"
				+ "						value2.max = 0; \n"
				+ "						value2.avg = 0; \n"
				+ "						checkboard" + GV_num + ".addElement(comb);\n"	
				+ "						class" + GV_num + ".put(comb,value2);\n"
				+ "					}\n "
				+ "				}\n";
		return operation;
	}

	//extract all the conditions for every single grouping variable
	public static String get_conditions(String GV_num){
		String condition = "";
		Vector<String> condis = new Vector<String>();
		condis = condition_check.get("class"+GV_num);
		
		for (int i = 0; i < condis.size(); i++) {
			condition += condis.get(i);
			Vector<String> temp = log_op.get("class"+String.valueOf(GV_num));
			if (i<temp.size()){
				if (temp.get(i) == "AND"){
					condition += "&&";
				}
				else if (temp.get(i) == "OR"){
					condition += "||";
				}
			}		
		}
		return condition;
	}
	//generate the output class for producing the result.
	public static void generate_class(){
		System.out.println(imports);
		System.out.println(class_name);
		
		System.out.println(class_main_front);		
		for (int i = 1; i<=Num_GV; i++) {
			GV_num  = String.valueOf(i);
			System.out.println(produce_value_class(GV_num));
			System.out.println(get_connection_front_String(GV_num));
			System.out.println(retrieve_data(GV_num));
			System.out.println(checkboard_operation(GV_num));
			System.out.println(get_connection_tail_String(GV_num));
		}
		
		//Print outputs.
		System.out.println("		//Print the output.");
		System.out.println("		System.out.println("+print_header(grouping_atts,atts)[0]+"\");");
		System.out.println("		System.out.println("+print_header(grouping_atts,atts)[1]+"\");");
		print_content(grouping_atts,atts);
		
		System.out.println(class_main_tail);
		
		produce_print_function();
		get_mf_structure_ga();
//		get_mf_structure_af(); 
		produce_value_table();
		System.out.println(class_name_tail);
	}
	
	//Produce the value_table
	public static void produce_value_table(){
		System.out.println(
				"	class Value{\n"
				+ "		public long sum = 0;\n"
				+ "		public long avg = 0;\n"
				+ "		public long max = 0;\n"
				+ "		public long min = 0;\n"
				+ "		public int count = 0;\n"
				+ "	}\n"
				);
	}
	
	// get the mf_structure
	public static void get_mf_structure_ga() {
		String ga = "";
		for (String t : grouping_atts) {
			switch (t) {
			case ("cust"):
				ga = ga + "		public String " + t + " = \"\";\n";
				break;
			case ("prod"):
				ga = ga + "		public String " + t + " = \"\";\n";
				break;
			case ("state"):
				ga = ga + "		public String " + t + " = \"\";\n";
				break;
			case ("year"):
				ga = ga + "		public int " + t + " = 0;\n";
				break;
			case ("month"):
				ga = ga + "		public int " + t + " = 0;\n";
				break;
			case ("day"):
				ga = ga + "		public int " + t + " = 0;\n";
				break;
			case ("quant"):
				ga = ga + "		public int " + t + " = 0;\n";
				break;
			}
		}
		System.out.println("	class "+ GA_class + "{\n" + ga + "	}");
	}
	
	public static void get_mf_structure_af(){
		String afs = "";
		Vector<String> check = new Vector<String>();
		for (String t:agg_funcs){
			if (!check.contains(t)){
				afs += "		public int " + t +" = 0;\n";
				check.add(t);
			}
		}
		System.out.println("\n	class "+ AF_class + "{\n" 
				+ "		mf_structure_ga gas;\n"
				+ afs + "	}\n");
	}
	
	// get selection attributes
	public static void get_S_Attributes() {
		String S_Predicate;
		String S_Attributes;
		File file = null;
		URL path = ClassLoader.getSystemResource("input.txt");
		if (path == null) {
			// The file was not found, insert error handling here
		}
		try {
			file = new File(path.toURI());
		} catch (URISyntaxException e1) {
			e1.printStackTrace();
		}
		try {
			br = new BufferedReader(new FileReader(file));
			while ((S_Predicate = br.readLine()) != null) {
				if (S_Predicate.equalsIgnoreCase("SELECT ATTRIBUTE(S):")) {
					if ((S_Attributes = br.readLine()) != null) {
						atts = S_Attributes.split(",");
					}
				}
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// get number of grouping variables
	public static void get_G_Number() {
		String S_Predicate;
		String Nums;
		af_check = new HashMap<String,Vector<String>>();
		condition_check = new HashMap<String,Vector<String>>();
		log_op = new HashMap<String,Vector<String>>();
		File file = null;
		URL path = ClassLoader.getSystemResource("input.txt");
		if (path == null) {
			// The file was not found, insert error handling here
		}
		try {
			file = new File(path.toURI());
		} catch (URISyntaxException e1) {
			e1.printStackTrace();
		}
		try {
			br = new BufferedReader(new FileReader(file));
			while ((S_Predicate = br.readLine()) != null) {
				if (S_Predicate
						.equalsIgnoreCase("NUMBER OF GROUPING VARIABLES(n):")) {
					if ((Nums = br.readLine()) != null) {
						Num_GV = Integer.valueOf(Nums);
						//initialize the HashMap for all aggregate function used to check later on.
						for (int i=1; i <= Num_GV; i++){
							aggs = new Vector<String>();							
							af_check.put("class"+String.valueOf(i), aggs);
							Vector<String> condition = new Vector<String>();
							condition_check.put("class"+String.valueOf(i), condition);
							Vector<String> ops = new Vector<String>();
							log_op.put("class"+String.valueOf(i), ops);
						}
					}
				}
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// get grouping attributes
	public static void get_G_Attributes() {
		String S_Predicate;
		String S_Attributes;
		File file = null;
		URL path = ClassLoader.getSystemResource("input.txt");
		if (path == null) {
			// The file was not found, insert error handling here
		}
		try {
			file = new File(path.toURI());
		} catch (URISyntaxException e1) {
			e1.printStackTrace();
		}
		try {
			br = new BufferedReader(new FileReader(file));
			while ((S_Predicate = br.readLine()) != null) {
				if (S_Predicate.equalsIgnoreCase("GROUPING ATTRIBUTES(V):")) {
					if ((S_Attributes = br.readLine()) != null) {
						grouping_atts = S_Attributes.split(",");
					}
				}
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// get aggregate functions
	public static void get_F_Aggregates() {
		String S_Predicate;
		String S_Attributes;
		File file = null;
		URL path = ClassLoader.getSystemResource("input.txt");
		if (path == null) {
			// The file was not found, insert error handling here
		}
		try {
			file = new File(path.toURI());
		} catch (URISyntaxException e1) {
			e1.printStackTrace();
		}
		try {
			br = new BufferedReader(new FileReader(file));
			while ((S_Predicate = br.readLine()) != null) {
				if (S_Predicate.equalsIgnoreCase("F-VECT([F]):")) {
					if ((S_Attributes = br.readLine()) != null) {
						agg_funcs = S_Attributes.split(",");
						
					}
				}
			}
			br.close();
			for (String t:agg_funcs){
				String[] temp = t.split("_");
				Vector<String> tv = new Vector<String>();
				for (String af:temp){
					tv.add(af);
				}	
				if (tv.contains("sum")) {
					af_check.get("class"+String.valueOf(tv.get(0))).add("sum");
				} else if (tv.contains("avg")) {
					af_check.get("class"+String.valueOf(tv.get(0))).add("avg");
				} else if (tv.contains("max")) {
					af_check.get("class"+String.valueOf(tv.get(0))).add("max");
				} else if (tv.contains("min")) {
					af_check.get("class"+String.valueOf(tv.get(0))).add("min");
				} else if (tv.contains("count")) {
					af_check.get("class"+String.valueOf(tv.get(0))).add("count");
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// predicate conditions
	public static void get_Conditions() {
		predicates = new Vector<String>();
		String S_Predicate;
		String S_Attributes;
		File file = null;
		URL path = ClassLoader.getSystemResource("input.txt");
		if (path == null) {
			// The file was not found, insert error handling here
		}
		try {
			file = new File(path.toURI());
		} catch (URISyntaxException e1) {
			e1.printStackTrace();
		}
		try {
			br = new BufferedReader(new FileReader(file));
			while ((S_Predicate = br.readLine()) != null) {
				if (S_Predicate.equalsIgnoreCase("SELECT CONDITION-VECT([σ]):")) {
					while ((S_Attributes = br.readLine()) != null) {
						if (S_Attributes != null) {
							String[] each_line = S_Attributes.split(",");
							predicates.addElement(each_line[0]);
						}
					}
				}
			}
			Vector<String> predi_convert = new Vector<String>();
			
			int classcount= 0;
			for (String a: predicates){
				classcount++;
				String[] line = a.split("AND");
				int count = line.length-1;
				for (int i=0; i<line.length ; i++){
					String[] temp=line[i].split("OR");
					//if there is no OR operator
					if (temp[0]==line[i]){
						char first = temp[0].charAt(0);
						int index=0;
						while (first == ' '){							
							index++;
							first = temp[0].charAt(index);
						}
						String sub = temp[0].substring(index);
						predi_convert.addElement(sub);
						if (count>0){
							log_op.get("class"+String.valueOf(classcount)).add("AND");
							count--;
						}
					}
					//if there is at least one OR operator
					else {
						for (int n=0 ; n<temp.length;n++){
							char first = temp[n].charAt(0);
							int index=0;
							while (first == ' '){
								index++;
								first = temp[n].charAt(index);
							}
							String sub = temp[n].substring(index);
							predi_convert.addElement(sub);
							//add the OR into vector
							if (n<temp.length-1){
								log_op.get("class"+String.valueOf(classcount)).add("OR");
							}
						}
					}
				}
			}
			br.close();
			for (String t: predi_convert){
				String[] temp = t.split(" ");
				Vector<String> parts = new Vector<String>();
				for (String i: temp){
					parts.add(i);
				}
				//get the part before the operator
				String[] temp_front = temp[0].split("_");
				Vector<String> righthand = new Vector<String>();
				for (int i = 2; i < temp.length ; i++){
					String[] subright = temp[i].split("_");
					for (String a : subright){
						righthand.add(a);
					}
				}
				int indexof;
				//if the right hand side contains other aggregate function
				if ((indexof = contain_agg_funs(righthand)) != -1){
					String right_num = righthand.get(indexof-1);
					//if there is any other number before the agg functions, ex. 2 * 2_avg_quant
					if (indexof!=1){
						String sub = "";
						for (int i = 0 ; i<indexof-1; i++){
							sub += righthand.get(i);
						}
						String condition = temp_front[1] + convert_operator(temp[1]) + sub + "class" + right_num + ".get(comb)." + righthand.get(indexof);
						condition_check.get("class"+temp_front[0]).add(condition);
					}
					else {
						String condition = temp_front[1] + convert_operator(temp[1]) + "class" + right_num + ".get(comb)." + righthand.get(indexof);
						condition_check.get("class"+temp_front[0]).add(condition);
					}
				}
				else {
					//if the left side contains day, month, year, or quant, then we keep the original operator
					if (temp_front[1].equals("day")||temp_front[1].equals("month")||temp_front[1].equals("year")||temp_front[1].equals("quant")){
						//if the operator is =, then change it into ==
						if (temp[1].equals("=")){
							String condition = temp_front[1] + "==" + "(" + temp[2] + ")";
							condition_check.get("class"+temp_front[0]).add(condition);
						}
						else {
							String condition = temp_front[1] + temp[1] + "(" + temp[2] + ")";
							condition_check.get("class"+temp_front[0]).add(condition);
						}
					}
					// else if the operator is =, we have to use equals function.
					else {
						String condition = temp_front[1] + convert_operator(temp[1]) + "(" + temp[2] + ")";
						if (temp[1].equals("!=")){
							condition = "!" + condition;
						}
						condition_check.get("class"+temp_front[0]).add(condition);
					}
				}
				
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String convert_operator(String op){
		switch (op) {
		case "=":
			return ".equals";
		case ">":
			return op;
		case "<":
			return op;
		case ">=":
			return op;
		case "<=":
			return op;
		case "!=":
			return ".equals";
		default:
			return null;
		}
	}

	public static int contain_agg_funs(Vector<String> vec){
		for (int i=0; i<vec.size();i++){
			switch (vec.get(i)){
			case "sum":
				return i;
			case "avg":
				return i;
			case "min":
				return i;
			case "max":
				return i;
			case "count":
				return i;
			default : 
					continue;
			}
		}
		return -1;
	}
	
	//New added ->0508
	public static void produce_print_function(){
		System.out.println(
				"	public String repeat_char(String ch, int count){\n"
				+ "		String result = \"\";\n"
				+ "		for(int j = 0;j<count;j++){\n"
				+ "			result = result + ch;\n"
				+ "		}\n"
				+ "		return result;\n"
				+ "	};\n"
				);
	}
	
		//Print header and separator => return two lines
	public static String[] print_header(String[] grouping_atts, String[] atts){
		
		String[] header = {"\"","\""};
		for(int i = 0; i<atts.length; i++){
			if(!atts[i].contains("_")){
				if(atts[i].equalsIgnoreCase("cust")){
					header[0] = header[0] + "CUSTOMER ";
					header[1] = header[1] + "======== ";
				} else if (atts[i].equalsIgnoreCase("prod")){
					header[0] = header[0] + "PRODUCT ";
					header[1] = header[1] + "======= ";
				} else {
					header[0] = header[0] + grouping_atts[i].toUpperCase() + " ";
					String tem = "";
					for(int temp = 0; temp<grouping_atts[i].length(); temp++){
						tem = tem + "=";
					}
					header[1] = header[1] + tem + " ";
				}
			} else{						
				if(atts[i].toUpperCase().contains("COUNT")){
					header[0] = header[0] + atts[i].toUpperCase()+" "; 
					header[1] = header[1] + "============= ";	
				} else {
					header[0] = header[0] + atts[i].toUpperCase()+" "; 
					header[1] = header[1] + "=========== ";	
				}
													
			}
		}
			return(header);		
	}

	//Print content of the output
	public static void print_content(String[] grouping_atts, String[] atts){				
		System.out.println("		for(String key : class1.keySet()){");
		for(int i = 0; i<atts.length; i++){
			if(!atts[i].contains("_")){
				int title_length = 6;		
				if (atts[i].equalsIgnoreCase("cust")){
					title_length = 9;
				} else if (atts[i].equalsIgnoreCase("prod")){
					title_length = 8;
				} else if (atts[i].equalsIgnoreCase("year")){
					title_length = 5;
				} else if (atts[i].equalsIgnoreCase("day")){
					title_length = 4;
				}						
				String print_atts = 
						"			System.out.print(key.split(\"_\")[" 
						+ i
						+ "] + "
						+ "pro.repeat_char(\" \","
						+ title_length
						+ " - key.split(\"_\")["
						+ i
						+ "].length()));\n";			
				System.out.print(print_atts);
			} else {		
				String class_num = atts[i].split("_")[0];
				String print_content = 
						"			System.out.print(pro.repeat_char(\" \", 9 - String.valueOf(class"
						+ class_num
						+ ".get(key)."
						+ atts[i].split("_")[1]
						+ ").length()) + class"
						+ class_num
						+ ".get(key)."
						+ atts[i].split("_")[1]
						+ " + \" \"); \n";
				System.out.print(print_content);
			}
		}
		System.out.println("			System.out.println();");
		System.out.println("		}");		
	}
	
}
