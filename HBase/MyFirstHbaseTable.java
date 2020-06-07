package hbaseTest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class MyFirstHbaseTable
{

	private static final String TABLE_NAME = "user";
	
	private static final String ROW_KEY = "row_key";
	private static final String CF_PERSONAL = "personal_details";
	private static final String CF_PROFESSIONAL = "prof_details";
	
	public static void updateDesignationAndSalary(String updatedRow,String newDesignation, double increasePercentage) throws IOException{
		
		Configuration config = HBaseConfiguration.create();
		
		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin())
		{
			Table userTable = connection.getTable(TableName.valueOf(TABLE_NAME));
			
		     // Instantiating Get class
		     Get g = new Get(Bytes.toBytes(updatedRow));
		     // Reading the data
		     Result result = userTable.get(g);
		    // Reading values from Result class object
		    byte [] value = result.getValue(Bytes.toBytes(CF_PROFESSIONAL),Bytes.toBytes("salary"));
		    String currecntSalary = Bytes.toString(value);
		    
		    double newSalary = Integer.parseInt(currecntSalary)* (1 + increasePercentage); 
			
			Put row = new Put(Bytes.toBytes(updatedRow));
			row.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes("Designation"),Bytes.toBytes(newDesignation));
			row.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes("salary"),Bytes.toBytes(newSalary+""));
			userTable.put(row);
			
			System.out.println("Data Updated");
			userTable.close();
		}

	}

	public static void main(String... args) throws IOException
	{

		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin())
		{
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(ROW_KEY));
			table.addFamily(new HColumnDescriptor(CF_PERSONAL).setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor(CF_PROFESSIONAL).setCompressionType(Algorithm.NONE));

			System.out.print("Creating table.... ");

			if (admin.tableExists(table.getTableName()))
			{
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
			
			Table userTable = connection.getTable(TableName.valueOf(TABLE_NAME));
			
			Put row1 = new Put(Bytes.toBytes("row1"));
			row1.addColumn(Bytes.toBytes(ROW_KEY), Bytes.toBytes("Empid"),Bytes.toBytes("1"));
			row1.addColumn(Bytes.toBytes(CF_PERSONAL), Bytes.toBytes("Name"),Bytes.toBytes("John"));
			row1.addColumn(Bytes.toBytes(CF_PERSONAL), Bytes.toBytes("City"),Bytes.toBytes("Boston"));
			row1.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes("Designation"),Bytes.toBytes("Manager"));
			row1.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes("salary"),Bytes.toBytes("150000"));
			userTable.put(row1);
			
			Put row2 = new Put(Bytes.toBytes("row2"));
			row2.addColumn(Bytes.toBytes(ROW_KEY), Bytes.toBytes("Empid"),Bytes.toBytes("2"));
			row2.addColumn(Bytes.toBytes(CF_PERSONAL), Bytes.toBytes("Name"),Bytes.toBytes("Mary"));
			row2.addColumn(Bytes.toBytes(CF_PERSONAL), Bytes.toBytes("City"),Bytes.toBytes("New York"));
			row2.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes("Designation"),Bytes.toBytes("Sr. Engineer"));
			row2.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes("salary"),Bytes.toBytes("130000"));
			userTable.put(row2);
			
			Put row3 = new Put(Bytes.toBytes("row3"));
			row3.addColumn(Bytes.toBytes(ROW_KEY), Bytes.toBytes("Empid"),Bytes.toBytes("3"));
			row3.addColumn(Bytes.toBytes(CF_PERSONAL), Bytes.toBytes("Name"),Bytes.toBytes("Bob"));
			row3.addColumn(Bytes.toBytes(CF_PERSONAL), Bytes.toBytes("City"),Bytes.toBytes("Fremont"));
			row3.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes("Designation"),Bytes.toBytes("Jr. Engineer"));
			row3.addColumn(Bytes.toBytes(CF_PROFESSIONAL), Bytes.toBytes("salary"),Bytes.toBytes("90000"));
			userTable.put(row3);

			System.out.println(" Done!");
			userTable.close();
			
			//Now programmatically promote Bob to Sr. Engineer position and increase his salary by 5%.
			updateDesignationAndSalary("row3","Sr. Engineer",0.05);
			
		}
	}
}