package de.tudresden.tim.exercise1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class DataHandler
{
	private static Connection connection;
	
	public static void main(String[] a) throws Exception
	{
		openConnection();
		CountryManager cm = new CountryManager();
//        cm.getCountryNames();
//		cm.listCountryCode();
//		cm.addRandomDataInNewPopulation();
//		cm.listNewPopulation();
//		cm.createTableNewPopulation();
//		cm.testname();
		
//		cm.listTotalPopulationFromCountry("BR");
//		cm.listTotalPopulationFromNewPopulation("BR");
//		cm.updateCurrentPopulation("BR");
//		cm.listTotalPopulationFromCountry("BR");
//		cm.listTotalPopulationFromNewPopulation("BR");

//		cm.getCurrentPopulationFromACountry("BR");
//		cm.listUpdatedTotalPopulation("BR");
		
		cm.addRandomDataInNewPopulationAndUpdateCurrentPopulation("NZ");
		
        closeConnection();
    }
	
	//CREATE TABLE NEWPOPULATION(COUNTRY_CODE varchar(3), AMOUNT int)
	
	//SLIDE 18.1
	//ALTER TABLE COUNTRY ADD CURRENT_POPULATION int;
	
	private static class CountryManager
	{
		public ResultSet executeSimpleQuery(String query)
		{
			try
			{
				return connection.createStatement().executeQuery(query);
			}
			catch (SQLException e)
			{
				System.out.println("There is an error in a query: " + query);
				e.printStackTrace();
			}
			return null;
		}
		
		public Integer getCurrentPopulationFromACountry(String countryCode) throws SQLException
		{
			PreparedStatement ps = connection.prepareStatement("SELECT CURRENT_POPULATION FROM COUNTRY WHERE CODE = ?");
			ps.setString(1, countryCode);
			ResultSet rs = ps.executeQuery();
			
			Integer result = 0;
			System.out.println("--------------- GETTING CURRENT POPULATION FROM COUNTRY  ------------------");
			while(rs.next())
			{
				result += rs.getInt("CURRENT_POPULATION");
			}
			System.out.println("Current population is: " + result);
			System.out.println("--------------- DONE GETTING CURRENT POPULATION FROM COUNTRY  ------------------");
			return result;
		}
		
		public void updateAllCurrentPopulation() throws SQLException
		{
			ArrayList<String> countryCodes = listAllCountryCodes();
			
			for(String s : countryCodes)
			{
				updateCurrentPopulation(s);
			}
		}

		public void updateCurrentPopulation(String countryCode) throws SQLException
		{
			Integer oldCountryValue = getPopulationFromACountry(countryCode);
			Integer newPopulationValue = getPopulationFromNewpopulation(countryCode);
			
			PreparedStatement ps = connection.prepareStatement("UPDATE COUNTRY " +
					"SET CURRENT_POPULATION = ? " +
					"WHERE CODE = ?");
			ps.setInt(1, oldCountryValue + newPopulationValue);
			ps.setString(2, countryCode);
			System.out.println("--------------- UPDATING ------------------");
			ps.executeUpdate();
		}
		
		public ArrayList<String> listAllCountryCodes() throws SQLException
		{
			ArrayList<String> countryCodes = new ArrayList<String>();
			ResultSet rs = executeSimpleQuery("SELECT CODE FROM COUNTRY");
			while(rs.next())
			{
				countryCodes.add(rs.getString("CODE"));
			}
			return countryCodes;
		}
		
		public Integer getPopulationFromACountry(String countryCode) throws SQLException
		{
			PreparedStatement ps = connection.prepareStatement("SELECT POPULATION FROM COUNTRY WHERE CODE = ?");
			ps.setString(1, countryCode);
			ResultSet rs = ps.executeQuery();
			
			Integer result = 0;
			System.out.println("--------------- GETTING POPULATION FROM COUNTRY  ------------------");
			while(rs.next())
			{
				result += rs.getInt("POPULATION");
			}
			System.out.println("Population is: " + result);
			System.out.println("--------------- DONE GETTING POPULATION FROM COUNTRY  ------------------");
			return result;
		}
		
		public Integer getPopulationFromNewpopulation(String countryCode) throws SQLException
		{
			PreparedStatement ps = connection.prepareStatement("SELECT AMOUNT FROM NEWPOPULATION WHERE COUNTRY_CODE = ?");
			ps.setString(1, countryCode);
			ResultSet rs = ps.executeQuery();
			
			Integer result = 0;
			System.out.println("--------------- GETTING AMOUNT FROM NEWPOPULATION  ------------------");
			while(rs.next())
			{
				result += rs.getInt("AMOUNT");
			}
			System.out.println("AMOUNT is: " + result);
			System.out.println("--------------- DONE GETTING AMOUNT FROM NEWPOPULATION  ------------------");
			return result;
		}
		/**
		 * This method returns a full population list
		 */
		public void listUpdatedTotalPopulation(String countryCode) throws SQLException
		{
			PreparedStatement ps =  connection.prepareStatement(
					"SELECT c.NAME as NAME, c.POPULATION, n.AMOUNT, (c.POPULATION + n.AMOUNT) as SUM " +
					"FROM COUNTRY c, NEWPOPULATION n " +
					"WHERE n.COUNTRY_CODE = c.CODE AND c.CODE = ?");
			ps.setString(1, countryCode);
			ResultSet rs = ps.executeQuery();
			
			while(rs.next())
			{
				System.out.println("Country Name: " + rs.getString("NAME") + " | Total Country population: " + rs.getString("SUM"));
			}
		}
		
		public void listTotalPopulationFromCountry(String countryCode) throws SQLException
		{
			PreparedStatement ps =  connection.prepareStatement("SELECT NAME, POPULATION FROM COUNTRY WHERE CODE = ?");
			ps.setString(1, countryCode);
			
			ResultSet rs = ps.executeQuery();
			
			System.out.println("------------- FROM COUNTRY --------------");
			while(rs.next())
			{
				System.out.println("Country Name: " + rs.getString("NAME")  + " | Country Population: " + rs.getInt("POPULATION"));
			}
			System.out.println("------------- DEBUG FROM COUNTRY --------------");
		}
		
		public void listTotalPopulationFromNewPopulation(String countryCode) throws SQLException
		{
			PreparedStatement ps =  connection.prepareStatement("SELECT COUNTRY_CODE, AMOUNT FROM NEWPOPULATION WHERE COUNTRY_CODE = ?");
			ps.setString(1, countryCode);
			
			ResultSet rs = ps.executeQuery();
			
			System.out.println("------------- FROM NEWPOPULATION --------------");
			while(rs.next())
			{
				System.out.println("Country code: " + rs.getString("COUNTRY_CODE")  + " | Amount: " + rs.getInt("AMOUNT"));
			}
			
			System.out.println("------------- DEBUG FROM NEWPOPULATION --------------");
		}
		
		public void createTableNewPopulation() throws SQLException
		{
			connection.createStatement().execute("CREATE TABLE NEWPOPULATION(COUNTRY_CODE varchar(3), AMOUNT int)");
		}
		
		@Deprecated
		public void addRandomDataInNewPopulation() throws SQLException
		{
			ResultSet rs = executeSimpleQuery("SELECT * FROM NEWPOPULATION");
			
			while(rs.next())
			{
				System.out.println("Country Name: " + rs.getString("COUNTRY_CODE") + " | Amount: " + rs.getInt("AMOUNT"));
			}
			
			Integer number = (int) Math.round((Math.random() * 10));
			PreparedStatement ps =  connection.prepareStatement("INSERT INTO NEWPOPULATION(COUNTRY_CODE, AMOUNT) VALUES(?, ?)");
			ps.setString(1, "NZ");
			ps.setInt(2, number);
			ps.executeUpdate();
			
		}
		
		public void addAmountInCurrentPopulation(Integer amount, String countryCode) throws SQLException
		{
			Integer currentPopulation = getCurrentPopulationFromACountry(countryCode);
			PreparedStatement ps = connection.prepareStatement("UPDATE COUNTRY " +
					"SET CURRENT_POPULATION = ? " +
					"WHERE CODE = ?");
			ps.setInt(1, currentPopulation + amount);
			ps.setString(2, countryCode);
			System.out.println("--------------- UPDATING ------------------");
			ps.executeUpdate();
		}
		
		public void addRandomDataInNewPopulationAndUpdateCurrentPopulation(String countryCode) throws SQLException
		{
			Integer number = (int) Math.round((Math.random() * 10));
			PreparedStatement ps =  connection.prepareStatement("INSERT INTO NEWPOPULATION(COUNTRY_CODE, AMOUNT) VALUES(?, ?)");
			ps.setString(1, countryCode);
			ps.setInt(2, number);
			ps.executeUpdate();
			addAmountInCurrentPopulation(number, countryCode);
		}
		
		public void testname(){
			for(int i = 0; i < 1000; i++)
			{
				Integer number = (int) Math.round((Math.random() * 10));
				if(number == 10)
				{
					System.out.println(i);
					break;
				}
				System.out.println(number);
			}
		}
		
		public void listCountryCode() throws SQLException
		{
			ResultSet rs = executeSimpleQuery("SELECT * FROM COUNTRY");
//			rs.
			while(rs.next())
			{
				System.out.println("Country Name: " + rs.getString("NAME"));
				System.out.println("Country Code: " + rs.getString("CODE"));
			}
		}
		
		public void listNewPopulation() throws SQLException
		{
			ResultSet rs = executeSimpleQuery("SELECT * FROM NEWPOPULATION");
			
			while(rs.next())
			{
				System.out.println("Country Name: " + rs.getString("COUNTRY_CODE") + " | Amount: " + rs.getInt("AMOUNT"));
			}
		}
		
		
		public void getCountryNames() throws SQLException
		{
			ResultSet rs = executeSimpleQuery("SELECT * FROM COUNTRY");
			while(rs.next())
			{
				System.out.println("COUNTRY NAMES: " + rs.getString(1));
			}
		}
	}

	public static void closeConnection()
	{
		try
		{
			connection.close();
		}
		catch (SQLException e)
		{
			System.out.println("It wasn't possible to close connection");
		}
	}
	public static void openConnection()
	{
		try
		{
			Class.forName("org.h2.Driver");
			connection = DriverManager.getConnection("jdbc:h2:~/test", "sa", "");
		}
		catch (ClassNotFoundException e)
		{
			System.out.println("Driver not found.");
		}
		catch (SQLException e)
		{
			System.out.println("It was not possible to create a connection");
			System.out.println("Please check out the database files");
		} 
	}
}
