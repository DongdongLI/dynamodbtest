package query;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;

import java.nio.channels.spi.AbstractSelector;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MoviesManagementQuery {

    public static void main(String[] args) {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable("Movies");

        Map<String, String> nameMap = new HashMap<String, String>();
        nameMap.put("#yr", "year");

        Map<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put(":yyyy", 1985);

        QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("#yr = :yyyy")
                .withNameMap(nameMap)
                .withValueMap(valueMap);

        ItemCollection<QueryOutcome> items = null;
        Iterator<Item> iterator = null;
        Item item = null;

        try {
            System.out.println("Movies from 1985...");
            items = table.query(querySpec);

            iterator = items.iterator();
            while(iterator.hasNext()) {
                item = iterator.next();
                System.out.println(item.getNumber("year") +": "+item.getString("title"));
            }

        }
        catch (Exception e) {
            System.err.println("Unable to get movies from 1985: ");
            System.err.println(e.getMessage());
        }
    }
}
