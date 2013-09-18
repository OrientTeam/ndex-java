package org.ndexbio.rest;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

@Test
public class NdexNetworkServiceTest {
  public void basicNetworkLoadingTest() throws Exception {
    OrientGraph orientGraph = new OrientGraph("memory:basicNetworkLoadingTest", "admin", "admin");

    final ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(NdexNetworkServiceTest.class
        .getResourceAsStream("/org/ndexbio/rest/testNetwork.jdex"));

    NdexNetworkService ndexNetworkService = new NdexNetworkService();
    final OrientVertex xUser = orientGraph.addVertex("xUser", (String) null);
    ndexNetworkService.createNetwork(xUser, rootNode, orientGraph);

    orientGraph.shutdown();
  }
}
