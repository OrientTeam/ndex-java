package org.ndexbio.rest;

import java.util.HashSet;
import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.OCluster;
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

  public void getNetworkTest() throws Exception {
    OrientGraph orientGraph = new OrientGraph("memory:getNetworkTest", "admin", "admin");

    final ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(NdexNetworkServiceTest.class
        .getResourceAsStream("/org/ndexbio/rest/testNetwork.jdex"));

    NdexNetworkService ndexNetworkService = new NdexNetworkService();
    final OrientVertex xUser = orientGraph.addVertex("xUser", (String) null);

    OrientVertex network = ndexNetworkService.createNetwork(xUser, rootNode, orientGraph);

    orientGraph.commit();

    ORID networkRid = (ORID) network.getId();

    ndexNetworkService.getNetwork(networkRid, orientGraph);
  }

  public void deleteNetworkTest() throws Exception {
    OrientGraph orientGraph = new OrientGraph("memory:deleteNetworkTest", "admin", "admin");

    final ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(NdexNetworkServiceTest.class
        .getResourceAsStream("/org/ndexbio/rest/testNetwork.jdex"));

    NdexNetworkService ndexNetworkService = new NdexNetworkService();
    final OrientVertex xUser = orientGraph.addVertex("xUser", (String) null);

    OrientVertex network = ndexNetworkService.createNetwork(xUser, rootNode, orientGraph);

    orientGraph.commit();

    ORID networkRid = (ORID) network.getId();

		orientGraph.getRawGraph().begin();
    ndexNetworkService.deleteNetwork(networkRid, orientGraph);
		orientGraph.getRawGraph().commit();

    ODatabaseDocumentTx databaseDocumentTx = orientGraph.getRawGraph();
    Set<String> clusterNames = new HashSet<String>(databaseDocumentTx.getClusterNames());

    for (String clusterName : clusterNames) {
			if(!(clusterName.startsWith("x") || clusterName.equals("default")))
				continue;

      int clusterId = databaseDocumentTx.getClusterIdByName(clusterName);
      OCluster cluster = databaseDocumentTx.getStorage().getClusterById(clusterId);
			Assert.assertEquals(cluster.getEntries(), 0);
    }
  }

	public void testFindNetwork() throws Exception {
		OrientGraph orientGraph = new OrientGraph("memory:testFindNetwork", "admin", "admin");

		final ObjectMapper objectMapper = new ObjectMapper();
		JsonNode rootNode = objectMapper.readTree(NdexNetworkServiceTest.class
						.getResourceAsStream("/org/ndexbio/rest/testNetwork.jdex"));

		NdexNetworkService ndexNetworkService = new NdexNetworkService();
		final OrientVertex xUser = orientGraph.addVertex("xUser", (String) null);

		ndexNetworkService.createNetwork(xUser, rootNode, orientGraph);

		orientGraph.commit();

		JsonNode result = ndexNetworkService.findNetworks("NCI_NATURE", 10, 0, orientGraph, objectMapper);
		Assert.assertEquals(((ArrayNode)result.get("networks")).size(), 1);
	}
}
