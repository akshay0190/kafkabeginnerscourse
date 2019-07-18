package com.github.aagarawal.kafka.tutorial3;


import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.elasticsearch.client.RequestOptions.DEFAULT;

/**
 * @author akshay.a
 */
public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient() {

//        https://guka1r63ex:d21setknns@kafka-cluster-5297324547.eu-west-1.bonsaisearch.net:443
        String hostname = "kafka-cluster-5297324547.eu-west-1.bonsaisearch.net";
        String username = "guka1r63ex";
        String password = "d21setknns";
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(username,password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname,443,"https")).setHttpClientConfigCallback(
                httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

       RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        String jsonString = "{ \"foo\": \"bar\" }";
        RestHighLevelClient client = createClient();
        IndexRequest indexRequest;
        indexRequest = new IndexRequest(
                "twitter",
                "tweets").
                source(jsonString, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, DEFAULT);
        String id = indexResponse.getId();
        logger.info(id);

        //close the client
        client.close();

    }
}
