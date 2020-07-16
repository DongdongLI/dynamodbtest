package cud;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.Delete;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

import java.util.Arrays;

public class MoviesManagementDelete {
    public static void main(String[] args) {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable("Movies");

        int year = 2015;
        String title = "The Big New Movie";

//        DeleteItemSpec spec = new DeleteItemSpec()
//                .withPrimaryKey("year", year, "title", title)
//                .withConditionExpression("info.rating <= :val")
//                .withValueMap(new ValueMap()
//                        .withNumber(":val", 5.0));

        DeleteItemSpec spec = new DeleteItemSpec()
                .withPrimaryKey("year", year, "title", title);

        try {
            System.out.println("Delete...");
            DeleteItemOutcome outcome = table.deleteItem(spec);

            System.out.println("Delete succeeded:\n" + outcome);

        }
        catch (Exception e) {
            System.err.println("Unable to Delete item: " + year + " " + title);
            System.err.println(e.getMessage());
        }
    }
}
