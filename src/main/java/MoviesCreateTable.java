import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.Arrays;

/*
* No need for read AWS credential if DynamoDB is running locally: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html*/

public class MoviesCreateTable {
    public static void main(String[] args) {
        AmazonDynamoDB client = AmazonDynamoDBAsyncClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);

        String tableName = "Movies";

        try {
            System.out.println("Creating table...");
            Table table = dynamoDB.createTable(tableName,
                Arrays.asList(
                    new KeySchemaElement("year", KeyType.HASH),
                    new KeySchemaElement("title", KeyType.RANGE)
                ),
                Arrays.asList(
                    new AttributeDefinition("year", ScalarAttributeType.N),
                    new AttributeDefinition("title", ScalarAttributeType.S)
                ),
                new ProvisionedThroughput(10L, 10L));

            table.waitForActive();
            System.out.println("Table created. Status: " + table.getDescription().getTableStatus());

        } catch (Exception e) {
            System.err.println("Unable to create table");
            System.err.println(e.getMessage());
        }
    }
}
