package database;

import java.sql.*;
import java.util.Date;
import java.util.Objects;
import java.util.Scanner;


public class db1 {



	public static void main(String[] args) {


		System.out.println("start");


		Connection c=null;
		PreparedStatement prepared_stmt=null;
		Statement stmt=null;
		ResultSet rs=null;
		try {
			c=DriverManager.getConnection("jdbc:postgresql://83.212.107.74/ergasia_baseis","postgres","baseisdedomenwn");
			System.out.println("Opened database successfully");

			stmt = c.createStatement();
			Scanner scan =new Scanner(System.in);


			while(true) {
				System.out.println("menu..,1.2.3.4...7 sql queries,press 0 to quit");
				String choice=scan.next();

				//while(!(Objects.equals(choice,"0")|Objects.equals(choice,"1")|Objects.equals(choice,"2")|Objects.equals(choice,"3")|Objects.equals(choice,"4")|Objects.equals(choice,"5")| Objects.equals(choice,"6")|Objects.equals(choice,"7"))){
				//	System.out.println("Invalid input.Press 0 or 1 or 2 ..");
				//	choice = scan.next();
				//}
				switch (choice) {
					case "0":
						break;
					case "1":

						//first sql querry
						String sql1 = " select type,count(type)from tickets inner join transactions on transactions.tickets_id=tickets.id group by tickets.type";


						rs = stmt.executeQuery(sql1);
						System.out.println("Execute query 1 successfully");
						ResultSetMetaData rsmd1 = rs.getMetaData();
						int columnsNumber1 = rsmd1.getColumnCount();
						while (rs.next()) {
							for (int i = 1; i <= columnsNumber1; i++) {
								if (i > 1) System.out.print(",  ");
								String columnValue = rs.getString(i);
								System.out.print(columnValue + " " + rsmd1.getColumnName(i));
							}
							System.out.println("");
						}
						rs.close();
						continue;
						//choice=scan.next();
						//-----------------------------------------------------------------------------------------------------------------------------------------------\\
					case "2":
						System.out.println(" ");
						//second sql query
						String sql2 = " select type, count (type),sum (price) as price from tickets group by tickets.type ORDER BY price DESC LIMIT 1;";


						rs = stmt.executeQuery(sql2);
						System.out.println("Execute query 2 successfully");
						ResultSetMetaData rsmd2 = rs.getMetaData();
						int columnsNumber2 = rsmd2.getColumnCount();
						while (rs.next()) {
							for (int i = 1; i <= columnsNumber2; i++) {
								if (i > 1) System.out.print(",  ");
								String columnValue = rs.getString(i);
								System.out.print(columnValue + " " + rsmd2.getColumnName(i));
							}
							System.out.println("");
						}
						rs.close();
						continue;
						//choice=scan.next();
						//------------------------------------------------------------------------------------------------------------------------------------------------\\

					case "3":
						System.out.println(" ");
						//third sql query
						String sql3 = "SELECT tickets.type,avg(price::money::numeric::float8)\r\n" +
								"FROM transactions\r\n" +
								"INNER JOIN customers ON customers.afm = transactions.customer_id\r\n" +
								"INNER JOIN tickets ON tickets.id = transactions.tickets_id\r\n" +
								"where 15 < age AND age  < 45\r\n" +
								"group by tickets.type; ";


						rs = stmt.executeQuery(sql3);
						System.out.println("Execute query 3 successfully");
						ResultSetMetaData rsmd3 = rs.getMetaData();
						int columnsNumber3 = rsmd3.getColumnCount();
						while (rs.next()) {
							for (int i = 1; i <= columnsNumber3; i++) {
								if (i > 1) System.out.print(",  ");
								String columnValue = rs.getString(i);
								System.out.print(columnValue + " " + rsmd3.getColumnName(i));
							}
							System.out.println("");
						}
						rs.close();
						continue;

						//------------------------------------------------------------------------------------------------------------------------------------------------\\

					case "4":
						System.out.println(" ");
						//fourth sql query
						String sql4 = "select tickets.procurer, tickets.type, transactions.tickets_id,count (tickets_id) as ticketssum\r\n" +
								"from transactions\r\n" +
								"INNER JOIN tickets\r\n" +
								"ON tickets.id=transactions.tickets_id\r\n" +
								"group by tickets_id,tickets.procurer,tickets.type\r\n" +
								"order by ticketssum desc; ";

						rs = stmt.executeQuery(sql4);
						System.out.println("Execute query 4 successfully");
						ResultSetMetaData rsmd4 = rs.getMetaData();
						int columnsNumber4 = rsmd4.getColumnCount();
						while (rs.next()) {
							for (int i = 1; i <= columnsNumber4; i++) {
								if (i > 1) System.out.print(",  ");
								String columnValue = rs.getString(i);
								System.out.print(columnValue + " " + rsmd4.getColumnName(i));
							}
							System.out.println("");
						}
						rs.close();
						continue;

						//------------------------------------------------------------------------------------------------------------------------------------------------\\
					case "5":
						System.out.println(" ");
						//fifth sql query
						String sql5 = "select customers.name,customers.afm, count(customers.afm) as customer_go,tickets.type\r\n" +
								"from tickets\r\n" +
								"INNER JOIN transactions ON transactions.tickets_id = tickets.id\r\n" +
								"INNER JOIN customers on transactions.customer_id = customers.afm\r\n" +
								"group by afm,tickets.type\r\n" +
								"ORDER BY customer_go desc; ";

						rs = stmt.executeQuery(sql5);
						System.out.println("Execute query 5 successfully");
						ResultSetMetaData rsmd5 = rs.getMetaData();
						int columnsNumber5 = rsmd5.getColumnCount();
						while (rs.next()) {
							for (int i = 1; i <= columnsNumber5; i++) {
								if (i > 1) System.out.print(",  ");
								String columnValue = rs.getString(i);
								System.out.print(columnValue + " " + rsmd5.getColumnName(i));
							}
							System.out.println("");
						}
						rs.close();
						continue;

//------------------------------------------------------------------------------------------------------------------------------------------------\\
					case "6":
						System.out.println(" ");
						//sixth sql query
						String sql6 = " select count(companies.afm) as eisitiria_pou_poulise,companies.afm,companies.name\r\n" +
								"from transactions\r\n" +
								"INNER JOIN tickets on tickets.id = transactions.tickets_id\r\n" +
								"INNER JOIN companies on tickets.procurer = companies.afm\r\n" +
								"group by companies.afm,companies.name\r\n" +
								"order by eisitiria_pou_poulise desc";

						rs = stmt.executeQuery(sql6);
						System.out.println("Execute query 6 successfully");
						ResultSetMetaData rsmd6 = rs.getMetaData();
						int columnsNumber6 = rsmd6.getColumnCount();
						while (rs.next()) {
							for (int i = 1; i <= columnsNumber6; i++) {
								if (i > 1) System.out.print(",  ");
								String columnValue = rs.getString(i);
								System.out.print(columnValue + " " + rsmd6.getColumnName(i));
							}
							System.out.println("");
						}
						rs.close();
						continue;
						//------------------------------------------------------------------------------------------------------------------------------------------------\\
					case "7":
						System.out.println(" ");
						//seventh sql query
						String sql7 = " select tickets.id,tickets.procurer,tickets.type,tickets.location,tickets.price,tickets.genre\r\n" +
								"from transactions\r\n" +
								"INNER JOIN tickets ON transactions.tickets_id = tickets.id\r\n" +
								"where  conf_date  > ? AND conf_date < CURRENT_DATE\r\n" +
								"group by tickets.id\r\n" +
								"; ";
						System.out.println("give date in the form  yyyy-mm-dd");
						Scanner scanQueryParameter=new Scanner(System.in);

						String strDate =scanQueryParameter.next();



						prepared_stmt=c.prepareStatement(sql7);
						System.out.println("**");

						prepared_stmt.setDate(1,java.sql.Date.valueOf(strDate));
						System.out.println("***");

						rs = prepared_stmt.executeQuery();
						System.out.println("Execute query 7 successfully");

						ResultSetMetaData rsmd7 = rs.getMetaData();
						int columnsNumber7 = rsmd7.getColumnCount();

						while (rs.next()) {




							for (int i = 1; i <= columnsNumber7; i++) {
								if (i > 1) System.out.print(",  ");
								String columnValue = rs.getString(i);
								System.out.print(columnValue + " " + rsmd7.getColumnName(i));
							}
							System.out.println("");
						}

						rs.close();
						continue;

				}
			}

		} catch (SQLException ex) {

		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (stmt != null) {
					stmt.close();
				}
				if (c != null) {
					c.close();
				}
			}
			catch (SQLException ex) {

			}

		}
	}
}
