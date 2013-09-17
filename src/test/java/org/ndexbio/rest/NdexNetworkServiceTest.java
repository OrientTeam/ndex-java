package org.ndexbio.rest;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

@Test
public class NdexNetworkServiceTest {
	public void basicNetworkLoadingTest() throws Exception {
		OrientGraph orientGraph = new OrientGraph("memory:basicNetworkLoadingTest", "admin", "admin");

		final ObjectMapper objectMapper = new ObjectMapper();
		JsonNode rootNode = objectMapper.readTree(NdexNetworkServiceTest.class.getResourceAsStream("/org/ndexbio/rest/testNetwork.jdex"));

		NdexNetworkService ndexNetworkService = new NdexNetworkService();
		ndexNetworkService.createNetwork(rootNode, orientGraph);

		orientGraph.shutdown();
	}
}