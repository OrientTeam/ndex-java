package org.ndexbio.rest;

import java.util.HashSet;
import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.OCluster;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

@Test
public class NdexNetworkServiceTest {
  private final String       jdexFile           = "/org/ndexbio/rest/testNetwork.jdex";
  private OrientGraph        orientGraph;
  private NdexNetworkService ndexNetworkService = new NdexNetworkService();

  @BeforeMethod
  public void beforeMethod() {
    ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx("memory:ndexNetworkServiceTest");
    databaseDocumentTx.create();

    orientGraph = new OrientGraph(databaseDocumentTx);
    ndexNetworkService.init(orientGraph);
  }

  @AfterMethod
  public void afterMethod() {
    orientGraph.drop();
  }

  public void basicNetworkLoadingTest() throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(NdexNetworkServiceTest.class.getResourceAsStream(jdexFile));

    final OrientVertex xUser = orientGraph.addVertex("xUser", (String) null);
    ndexNetworkService.createNetwork(xUser, rootNode, orientGraph);
    orientGraph.commit();
  }

  public void getNetworkTest() throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(NdexNetworkServiceTest.class.getResourceAsStream(jdexFile));

    final OrientVertex xUser = orientGraph.addVertex("xUser", (String) null);

    OrientVertex network = ndexNetworkService.createNetwork(xUser, rootNode, orientGraph);

    orientGraph.commit();

    ORID networkRid = (ORID) network.getId();

    ndexNetworkService.getNetwork(networkRid, orientGraph);
  }

  public void deleteNetworkTest() throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(NdexNetworkServiceTest.class.getResourceAsStream(jdexFile));

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
      if (!(clusterName.startsWith("x") || clusterName.equals("default")) || clusterName.equals("xuser"))
        continue;

      int clusterId = databaseDocumentTx.getClusterIdByName(clusterName);
      OCluster cluster = databaseDocumentTx.getStorage().getClusterById(clusterId);
      Assert.assertEquals(cluster.getEntries(), 0);
    }

    int userClusterId = databaseDocumentTx.getClusterIdByName("xUser");
    OCluster userCluster = databaseDocumentTx.getStorage().getClusterById(userClusterId);
    Assert.assertEquals(userCluster.getEntries(), 1);

  }

  public void testFindNetwork() throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(NdexNetworkServiceTest.class.getResourceAsStream(jdexFile));

    final OrientVertex xUser = orientGraph.addVertex("xUser", (String) null);

    ndexNetworkService.createNetwork(xUser, rootNode, orientGraph);

    orientGraph.commit();

    JsonNode result = ndexNetworkService.findNetworks("NCI_NATURE", 10, 0, orientGraph, objectMapper);
    Assert.assertEquals(((ArrayNode) result.get("networks")).size(), 1);
  }

  public void testGetNetworkByEdges() throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(NdexNetworkServiceTest.class.getResourceAsStream(jdexFile));

    final OrientVertex xUser = orientGraph.addVertex("xUser", (String) null);

    OrientVertex network = ndexNetworkService.createNetwork(xUser, rootNode, orientGraph);
    orientGraph.commit();

    final ORID networkId = network.getIdentity();

    ndexNetworkService.getNetworkByEdges(networkId, 0, 100, orientGraph, objectMapper);
  }

  public void testGetNetworkByNodes() throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(NdexNetworkServiceTest.class.getResourceAsStream(jdexFile));

    final OrientVertex xUser = orientGraph.addVertex("xUser", (String) null);

    OrientVertex network = ndexNetworkService.createNetwork(xUser, rootNode, orientGraph);
    orientGraph.commit();

    final ORID networkId = network.getIdentity();

    ndexNetworkService.getNetworkByNodes(networkId, 0, 100, orientGraph, objectMapper);
  }
}
