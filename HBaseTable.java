package assignment18.session2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTable {
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		Configuration conf=HBaseConfiguration.create();
		HBaseAdmin admin=new HBaseAdmin(conf);
		//creating table "customer" with column family "details"
		HTableDescriptor des=new HTableDescriptor(Bytes.toBytes("customer"));
		des.addFamily(new HColumnDescriptor("details"));

		if(admin.tableExists("customer")){
			System.out.println("Table Already exists!");
			admin.disableTable("customer");
			admin.deleteTable("customer");
			System.out.println("Table: customer deleted");
		}
		admin.createTable(des);	
		System.out.println("Table: customer sucessfully created");
		
		//inserting records
		HTable table = new HTable(conf, "customer");
		Put put = new Put(Bytes.toBytes("1"));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("name"), Bytes.toBytes("Amit"));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("location"), Bytes.toBytes("IND"));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("age"), Bytes.toBytes("18"));
		table.put(put);
		System.out.println("1st record inserted Successfully!!!");

		put = new Put(Bytes.toBytes("2"));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("name"), Bytes.toBytes("Sumit"));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("location"), Bytes.toBytes("PAK"));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("age"), Bytes.toBytes("20"));
		table.put(put);
		System.out.println("2nd record inserted Successfully!!!");
		
		put = new Put(Bytes.toBytes("3"));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("name"), Bytes.toBytes("Rohit"));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("location"), Bytes.toBytes("AUS"));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("age"), Bytes.toBytes("26"));
		table.put(put);
		System.out.println("3rd record inserted Successfully!!!");
		
		put = new Put(Bytes.toBytes("4"));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("name"), Bytes.toBytes("Namit"));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("location"), Bytes.toBytes("UK"));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("age"), Bytes.toBytes("24"));
		table.put(put);
		System.out.println("4th record inserted Successfully!!!");
		
		//scanning the table
		Scan sc=new Scan();
		ResultScanner rs=table.getScanner(sc);
		System.out.println("Getting all records for table customer\n");
		for(Result r:rs){
	        for(KeyValue kv : r.raw()){
	           System.out.print(new String(kv.getRow()) + " ");
	           System.out.print(new String(kv.getFamily()) + ":");
	           System.out.print(new String(kv.getQualifier()) + " ");
	           System.out.print(kv.getTimestamp() + " ");
	           System.out.println(new String(kv.getValue()));
	        }
		}
	}
}
