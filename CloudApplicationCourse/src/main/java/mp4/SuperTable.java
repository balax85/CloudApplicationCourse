package mp4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class SuperTable{

   public static void main(String[] args) throws IOException {
	   
	 try {  

      // Instantiate Configuration class
     
      Configuration con = HBaseConfiguration.create();

      // Instaniate HBaseAdmin class
     
      HBaseAdmin admin = new HBaseAdmin(con);
     
      // Instantiate table descriptor class
     
      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("powers"));

      // Add column families to table descriptor
     
      tableDescriptor.addFamily(new HColumnDescriptor("personal"));
      tableDescriptor.addFamily(new HColumnDescriptor("professional"));

      // Execute the table through admin
     
      admin.createTable(tableDescriptor);

      // Instantiating HTable class
     
       HTable hTable = new HTable(con, "powers");
    
      // Repeat these steps as many times as necessary

          // Instantiating Put class
              // Hint: Accepts a row name

                // Add values using add() method
              // Hints: Accepts column family name, qualifier/row name ,value
             
            Put p1 = new Put(Bytes.toBytes("row1"));
              
            p1.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"),Bytes.toBytes("superman"));
            p1.add(Bytes.toBytes("personal"),Bytes.toBytes("power"),Bytes.toBytes("strength"));
            p1.add(Bytes.toBytes("professional"),Bytes.toBytes("name"), Bytes.toBytes("clark"));
            p1.add(Bytes.toBytes("professional"),Bytes.toBytes("xp"), Bytes.toBytes("100"));
            
            Put p2 = new Put(Bytes.toBytes("row2"));
            
            p2.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"),Bytes.toBytes("batman"));
            p2.add(Bytes.toBytes("personal"),Bytes.toBytes("power"),Bytes.toBytes("money"));
            p2.add(Bytes.toBytes("professional"),Bytes.toBytes("name"), Bytes.toBytes("bruce"));
            p2.add(Bytes.toBytes("professional"),Bytes.toBytes("xp"), Bytes.toBytes("50"));
            
            Put p3 = new Put(Bytes.toBytes("row3"));
            
            p3.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"),Bytes.toBytes("wolverine"));
            p3.add(Bytes.toBytes("personal"),Bytes.toBytes("power"),Bytes.toBytes("healing"));
            p3.add(Bytes.toBytes("professional"),Bytes.toBytes("name"), Bytes.toBytes("logan"));
            p3.add(Bytes.toBytes("professional"),Bytes.toBytes("xp"), Bytes.toBytes("75"));

      // Save the table
            hTable.put(p1);
            hTable.put(p2);
            hTable.put(p3);

      // Close table
     
       hTable.close();

      // Instantiate the Scan class
    
     Scan scan = new Scan();
     
     hTable = new HTable(con, "powers");
     
      // Scan the required columns
     
       scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("hero"));

      // Get the scan result
     
       ResultScanner scanner = hTable.getScanner(scan);

      // Read values from scan result
      // Print scan result
       
       //keyvalues={row1/personal:hero/1442104171204/Put/vlen=8/seqid=0}
       
       for (Result result = scanner.next(); result != null; result = scanner
				.next())
			System.out.println(result);
       
//      for (Result result = scanner.next(); result != null; result = scanner.next())
//
//      System.out.println("Found row : " + result);
 
      // Close the scanner
     
      scanner.close();
  
      // Htable closer
      
      hTable.close();
      
	 } catch (Exception e) {
		 e.printStackTrace();
		 throw new RuntimeException(e);
	 }
   }
}