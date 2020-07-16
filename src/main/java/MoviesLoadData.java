import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.net.URL;
import java.util.Iterator;

public class MoviesLoadData {
    public static void main(String[] args) throws Exception {
        AmazonDynamoDB client = AmazonDynamoDBAsyncClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable("Movies");
        // https://stackoverflow.com/questions/1480398/java-reading-a-file-from-current-directory
        URL path = MoviesLoadData.class.getResource("moviedata.json");
        File f = new File(path.getFile());

        JsonParser jsonParser = new JsonFactory().createParser(f);

        JsonNode rootNode = new ObjectMapper().readTree(jsonParser);
        Iterator<JsonNode> iterator = rootNode.iterator();

        ObjectNode currentNode;

        while(iterator.hasNext()) {
            currentNode = (ObjectNode)iterator.next();

            int year = currentNode.path("year").asInt();
            String title = currentNode.path("title").asText();

            try {
                table.putItem(
                        new Item().withPrimaryKey("year", year, "title", title)
                                .withJSON("info", currentNode.path("info").toString())
                );
                System.out.println("Movie added: "+year+" "+title);
            } catch (Exception e) {
                System.err.println("Unable to add movie: "+year+" "+title);
                System.err.println(e.getMessage());
                break;
            }
        }
        jsonParser.close();
    }
}
