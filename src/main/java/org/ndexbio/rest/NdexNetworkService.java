package org.ndexbio.rest;

import java.util.*;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.ndexbio.rest.utils.RidConverter;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientElement;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * @since 9/17/13
 */
public class NdexNetworkService {
  public static final NdexNetworkService INSTANCE = new NdexNetworkService();

  public synchronized void init(OrientBaseGraph orientGraph) {
    orientGraph.getRawGraph().commit();

    if (orientGraph.getVertexType("xNetwork") == null) {
      OClass networkClass = orientGraph.createVertexType("xNetwork");
      networkClass.createProperty("ndexEdges", OType.LINKSET);
      networkClass.createProperty("format", OType.STRING);
      networkClass.createProperty("properties", OType.EMBEDDEDMAP);
      networkClass.createProperty("edgesCount", OType.INTEGER);
      networkClass.createProperty("nodesCount", OType.INTEGER);
    }

    if (orientGraph.getVertexType("xNameSpace") == null) {
      OClass nameSpaceClass = orientGraph.createVertexType("xNameSpace");
      nameSpaceClass.createProperty("jdex_id", OType.STRING);
      nameSpaceClass.createProperty("prefix", OType.STRING);
      nameSpaceClass.createProperty("uri", OType.STRING);
    }

    if (orientGraph.getVertexType("xUser") == null) {
      OClass userClass = orientGraph.createVertexType("xUser");
    }

    if (orientGraph.getVertexType("xTerm") == null) {
      OClass termClass = orientGraph.createVertexType("xTerm");
    }

    if (orientGraph.getVertexType("xBaseTerm") == null) {
      OClass baseTermClass = orientGraph.createVertexType("xBaseTerm", "xTerm");
      baseTermClass.createProperty("jdex_id", OType.STRING);
      baseTermClass.createProperty("name", OType.STRING);
    }

    if (orientGraph.getVertexType("xFunctionTerm") == null) {
      OClass functionTermClass = orientGraph.createVertexType("xFunctionTerm", "xTerm");
      functionTermClass.createProperty("linkParameters", OType.LINKSET);
      functionTermClass.createProperty("textParameters", OType.EMBEDDEDSET);

      functionTermClass.createProperty("termFunction", OType.LINK);
    }

    if (orientGraph.getVertexType("xNode") == null) {
      OClass nodeClass = orientGraph.createVertexType("xNode");
      nodeClass.createProperty("name", OType.STRING);
      nodeClass.createProperty("jdex_id", OType.STRING);
      nodeClass.createProperty("represents", OType.LINK);
    }

    if (orientGraph.getEdgeType("xEdge") == null) {
      OClass edgeClass = orientGraph.createEdgeType("xEdge");
      edgeClass.createProperty("p", OType.LINK);
      edgeClass.createProperty("s", OType.LINK);
    }

    if (orientGraph.getVertexType("xCitation") == null) {
      OClass citationClass = orientGraph.createVertexType("xCitation");
      citationClass.createProperty("identifier", OType.STRING);
      citationClass.createProperty("type", OType.STRING);
      citationClass.createProperty("title", OType.STRING);
      citationClass.createProperty("contributors", OType.EMBEDDEDLIST);
    }

    if (orientGraph.getVertexType("xSupport") == null) {
      OClass supportClass = orientGraph.createVertexType("xSupport");
      supportClass.createProperty("jdex_id", OType.STRING);
      supportClass.createProperty("text", OType.STRING);
    }
  }

  public synchronized OrientVertex createNetwork(OrientVertex owner, JsonNode networkJDEx, OrientBaseGraph orientGraph) {
    final OrientVertex network = orientGraph.addVertex("xNetwork", (String) null);
    owner.addEdge("xOwnsNetwork", network);

    if (networkJDEx.get("format") != null)
      network.setProperty("format", networkJDEx.get("format").asText());

    final HashMap<String, OrientVertex> networkIndex = new HashMap<String, OrientVertex>();
    createNameSpaces(network, networkJDEx, orientGraph, networkIndex);
    createTerms(networkJDEx, orientGraph, network, networkIndex);
    createProperties(networkJDEx, network);
    createNodes(network, networkJDEx, orientGraph, networkIndex);
    createSupports(networkJDEx, orientGraph, network, networkIndex);
    createCitations(networkJDEx, orientGraph, network, networkIndex);
    createEdges(networkJDEx, network, networkIndex);

    network.save();

    return network;
  }

  public synchronized boolean deleteNetwork(ORID networkRid, OrientBaseGraph orientGraph) {
    if (orientGraph.getVertex(networkRid) == null)
      return false;

    ODatabaseDocumentTx databaseDocumentTx = orientGraph.getRawGraph();
    List<ODocument> allNetworkRecords = databaseDocumentTx.query(new OSQLSynchQuery<Object>("select @rid from (TRAVERSE * FROM "
        + networkRid + ") where @class <> 'xUser'"));

    for (ODocument networkRecordId : allNetworkRecords) {
      ORID recordId = networkRecordId.field("rid", OType.LINK);
      OrientElement element = orientGraph.getElement(recordId);
      if (element != null)
        element.remove();
    }

    return true;
  }

  public ObjectNode findNetworks(String searchExpression, int limit, int offset, OrientBaseGraph orientGraph,
      ObjectMapper objectMapper) {
    int start = offset * limit;

    final String descriptors = "properties.title as title, @rid as rid, nodesCount as nodeCount, edgesCount as edgeCount";
    String where_clause = "";
    if (where_clause.length() > 0)
      where_clause = " where properties.title.toUpperCase() like '%" + searchExpression
          + "%' OR properties.description.toUpperCase() like '%" + searchExpression + "%'";

    final String query = "select " + descriptors + " from xNetwork " + where_clause + " order by creation_date desc skip " + start
        + " limit " + limit;
    List<ODocument> networks = orientGraph.getRawGraph().query(new OSQLSynchQuery<ODocument>(query));

    ObjectNode result = objectMapper.createObjectNode();
    ArrayNode jsonNetworks = objectMapper.createArrayNode();
    result.put("networks", jsonNetworks);

    for (ODocument document : networks) {
      ObjectNode network = objectMapper.createObjectNode();
      jsonNetworks.add(network);

      network.put("title", document.<String> field("title"));
      network.put("jid", RidConverter.convertFromRID(document.<ORID> field("rid", OType.LINK)));
      network.put("nodeCount", document.<Integer> field("nodeCount"));
      network.put("edgesCount", document.<Integer> field("edgesCount"));
    }

    result.put("blockAmount", 5);

    return result;
  }

  public JsonNode getNetwork(ORID networkRid, OrientBaseGraph orientGraph) {
    final OrientVertex vNetwork = orientGraph.getVertex(networkRid);
    if (vNetwork == null)
      return null;

    ObjectMapper mapper = new ObjectMapper();
    final ObjectNode result = mapper.createObjectNode();

    final Map<String, String> propertiesMap = vNetwork.getProperty("properties");
    result.put("title", propertiesMap.get("title"));

    final ObjectNode namespaces = mapper.createObjectNode();
    result.put("namespaces", namespaces);

    readNamespaces(vNetwork, mapper, namespaces);

    final ObjectNode terms = mapper.createObjectNode();
    result.put("terms", terms);

    readTerms(vNetwork, mapper, terms);

    final ObjectNode nodes = mapper.createObjectNode();
    result.put("nodes", nodes);

    readNodes(orientGraph, vNetwork, mapper, nodes);

    final ArrayNode edges = mapper.createArrayNode();
    result.put("edges", edges);

    readEdges(orientGraph, vNetwork, mapper, edges);

    return result;
  }

  public JsonNode getNetworkByEdges(ORID networkId, int offset, int limit, OrientBaseGraph orientGraph, ObjectMapper objectMapper) {
    ObjectNode result = objectMapper.createObjectNode();

    Vertex vNetwork = orientGraph.getVertex(networkId);
    if (vNetwork == null)
      throw new IllegalStateException("Network with with id " + networkId + " was not found.");

    final Map<String, String> properties = vNetwork.<Map<String, String>> getProperty("properties");
    result.put("title", properties.get("title"));

    ObjectNode terms = objectMapper.createObjectNode();
    result.put("terms", terms);

    readTerms(vNetwork, objectMapper, terms);

    ObjectNode nodes = objectMapper.createObjectNode();
    result.put("nodes", nodes);

    readNodes(orientGraph, vNetwork, objectMapper, nodes);

    ArrayNode edges = objectMapper.createArrayNode();
    result.put("edges", edges);

    final Set<OIdentifiable> ndexEdges = vNetwork.getProperty("ndexEdges");
    final int edgesCount = ndexEdges.size();

    final int blockAmount = (int) Math.ceil(((double) edgesCount) / limit);
    result.put("blockAmount", blockAmount);

    final int start = offset * limit;

    int counter = 0;
    for (OIdentifiable ndexEdge : ndexEdges) {
      if (counter >= start) {
        final ObjectNode edge = objectMapper.createObjectNode();
        edges.add(edge);

        readEdge(orientGraph.getEdge(ndexEdge), edge, orientGraph);
      }
      counter++;

      if (counter >= start + limit)
        break;
    }

    return result;
  }

  public JsonNode getNetworkByNodes(ORID networkId, int offset, int limit, OrientBaseGraph orientGraph, ObjectMapper objectMapper) {
    ObjectNode result = objectMapper.createObjectNode();

    OrientVertex vNetwork = orientGraph.getVertex(networkId);
    if (vNetwork == null)
      throw new IllegalStateException("Network with with id " + networkId + " was not found.");

    final Map<String, String> properties = vNetwork.<Map<String, String>> getProperty("properties");
    result.put("title", properties.get("title"));

    ObjectNode terms = objectMapper.createObjectNode();
    result.put("terms", terms);

    readTerms(vNetwork, objectMapper, terms);

    int nodeCount = vNetwork.<Integer> getProperty("nodesCount");
    Iterable<Vertex> vNodes = vNetwork.getVertices(Direction.OUT, "nodes");

    final int blockAmount = (int) Math.ceil(((double) nodeCount) / limit);
    result.put("blockAmount", blockAmount);
    final int start = offset * limit;

    ObjectNode nodes = objectMapper.createObjectNode();
    result.put("nodes", nodes);

    int counter = 0;
    for (Vertex vNode : vNodes) {
      if (counter >= start) {
        final ObjectNode node = objectMapper.createObjectNode();
        nodes.put(vNode.<String> getProperty("jdex_id"), node);

        readNode(orientGraph, vNode, node);
      }
      counter++;

      if (counter >= limit + start)
        break;
    }

    return result;
  }

  private void readEdges(OrientBaseGraph orientGraph, OrientVertex vNetwork, ObjectMapper mapper, ArrayNode edges) {
    Set<OIdentifiable> dEdges = vNetwork.getProperty("ndexEdges");
    for (OIdentifiable dEdge : dEdges) {
      Edge eEdge = orientGraph.getEdge(dEdge);

      ObjectNode edge = mapper.createObjectNode();
      edges.add(edge);

      readEdge(eEdge, edge, orientGraph);
    }
  }

  private void readEdge(Edge eEdge, ObjectNode edge, OrientBaseGraph orientGraph) {
    Vertex vPredicate = orientGraph.getVertex(eEdge.getProperty("p"));
    edge.put("p", vPredicate.<String> getProperty("jdex_id"));

    Vertex vSubject = eEdge.getVertex(Direction.OUT);
    Vertex vObject = eEdge.getVertex(Direction.IN);

    edge.put("s", vSubject.<String> getProperty("jdex_id"));
    edge.put("o", vObject.<String> getProperty("jdex_id"));
    edge.put("jid", RidConverter.convertFromRID((ORID) eEdge.getId()));
  }

  private void readNodes(OrientBaseGraph orientGraph, Vertex vNetwork, ObjectMapper mapper, ObjectNode nodes) {
    Iterable<Edge> eNodes = vNetwork.getEdges(Direction.OUT);
    for (Edge eNode : eNodes) {
      Vertex vNode = eNode.getVertex(Direction.IN);

      ObjectNode node = mapper.createObjectNode();
      nodes.put(vNode.<String> getProperty("jdex_id"), node);

      readNode(orientGraph, vNode, node);
    }
  }

  private void readNode(OrientBaseGraph orientGraph, Vertex vNode, ObjectNode node) {
    node.put("name", vNode.<String> getProperty("name"));
    node.put("jid", RidConverter.convertFromRID((ORID) vNode.getId()));

    final OIdentifiable dRepresents = vNode.getProperty("represents");
    if (dRepresents != null) {
      final Vertex vRepresents = orientGraph.getVertex(dRepresents);
      node.put("represents", vRepresents.<String> getProperty("jdex_id"));
    }
  }

  private void readTerms(Vertex vNetwork, ObjectMapper mapper, ObjectNode terms) {
    Iterable<Edge> eTerms = vNetwork.getEdges(Direction.OUT, "terms");
    for (Edge eTerm : eTerms) {
      Vertex vTerm = eTerm.getVertex(Direction.IN);

      final ObjectNode term = mapper.createObjectNode();
      terms.put(vTerm.<String> getProperty("jdex_id"), term);

      term.put("name", vTerm.<String> getProperty("name"));
      term.put("jid", RidConverter.convertFromRID((ORID) vTerm.getId()));
    }
  }

  private void readNamespaces(OrientVertex vNetwork, ObjectMapper mapper, ObjectNode namespaces) {
    Iterable<Edge> edges = vNetwork.getEdges(Direction.OUT, "namespaces");
    for (Edge edge : edges) {
      ObjectNode namespace = mapper.createObjectNode();
      Vertex vNs = edge.getVertex(Direction.IN);

      namespaces.put(vNs.<String> getProperty("jdex_id"), namespace);
      namespace.put("prefix", vNs.<String> getProperty("prefix"));
      namespace.put("jid", RidConverter.convertFromRID((ORID) vNs.getId()));
      namespace.put("uri", vNs.<String> getProperty("uri"));
    }
  }

  private void createEdges(JsonNode networkJDEx, OrientVertex network, HashMap<String, OrientVertex> networkIndex) {
    final Set<ODocument> allEdges = new HashSet<ODocument>();

    final JsonNode edges = networkJDEx.get("edges");
    final Iterator<String> iterator = edges.getFieldNames();
    while (iterator.hasNext()) {
      final JsonNode edgeJDEx = edges.get(iterator.next());

      final OrientVertex subject = loadFromIndex(networkIndex, edgeJDEx, "s");
      final OrientVertex object = loadFromIndex(networkIndex, edgeJDEx, "o");
      final OrientEdge edge = subject.addEdge("xEdge", object, null, null, (Object[]) null);

      edge.setProperty("p", loadFromIndex(networkIndex, edgeJDEx, "p").getRecord());
      edge.setProperty("n", network.getRecord());

      edge.save();

      allEdges.add(edge.getRecord());
    }

    network.setProperty("ndexEdges", allEdges);
    network.setProperty("edgesCount", allEdges.size());
  }

  private void createCitations(JsonNode networkJDEx, OrientBaseGraph orientGraph, OrientVertex network,
      HashMap<String, OrientVertex> networkIndex) {
    JsonNode citations = networkJDEx.get("citations");
    Iterator<String> citationsIterator = citations.getFieldNames();
    while (citationsIterator.hasNext()) {
      String index = citationsIterator.next();
      JsonNode citation = citations.get(index);

      OrientVertex vCitation = orientGraph.addVertex("xCitation", (String) null);
      if (citation.get("identifier") != null)
        vCitation.setProperty("identifier", citation.get("identifier").asText());

      if (citation.get("type") != null)
        vCitation.setProperty("type", citation.get("type").asText());

      if (citation.get("title") != null)
        vCitation.setProperty("title", citation.get("title").asText());

      vCitation.setProperty("contributors", asStringList(citation.get("contributors")));

      vCitation.setProperty("jdex_id", index);

      vCitation.save();

      network.addEdge("citations", vCitation);

      networkIndex.put(index, vCitation);
    }
  }

  private List<String> asStringList(JsonNode contributors) {
    List<String> result = new ArrayList<String>();
    for (JsonNode contributor : contributors) {
      result.add(contributor.asText());
    }

    return result;
  }

  private void createSupports(JsonNode networkJDEx, OrientBaseGraph orientGraph, OrientVertex network,
      HashMap<String, OrientVertex> networkIndex) {
    JsonNode supports = networkJDEx.get("supports");
    Iterator<String> supportsIterator = supports.getFieldNames();
    while (supportsIterator.hasNext()) {
      String index = supportsIterator.next();
      JsonNode support = supports.get(index);
      OrientVertex vSupport = orientGraph.addVertex("xSupport", (String) null);
      vSupport.setProperty("jdex_id", index);
      if (support.get("text") != null)
        vSupport.setProperty("text", support.get("text").asText());

      vSupport.save();

      network.addEdge("supports", vSupport);

      networkIndex.put(index, vSupport);
    }
  }

  private void createNameSpaces(OrientVertex network, JsonNode networkJDEx, OrientBaseGraph orientGraph,
      HashMap<String, OrientVertex> networkIndex) {
    JsonNode namespaces = networkJDEx.get("namespaces");
    Iterator<String> namespacesIterator = namespaces.getFieldNames();

    while (namespacesIterator.hasNext()) {
      String index = namespacesIterator.next();
      JsonNode namespace = namespaces.get(index);

      OrientVertex vNamespace = orientGraph.addVertex("xNameSpace", (String) null);
      vNamespace.setProperty("jdex_id", index);
      if (namespace.get("prefix") != null)
        vNamespace.setProperty("prefix", namespace.get("prefix").asText());

      vNamespace.setProperty("uri", namespace.get("uri").asText());

      vNamespace.save();

      network.addEdge("namespaces", vNamespace);

      networkIndex.put(index, vNamespace);
    }
  }

  private void createNodes(OrientVertex network, JsonNode networkJDEx, OrientBaseGraph orientGraph,
      HashMap<String, OrientVertex> networkIndex) {
    JsonNode nodes = networkJDEx.get("nodes");
    Iterator<String> nodesIterator = nodes.getFieldNames();
    int nodesCount = 0;

    while (nodesIterator.hasNext()) {
      String index = nodesIterator.next();
      JsonNode node = nodes.get(index);

      OrientVertex vNode = orientGraph.addVertex("xNode", (String) null);
      if (node.get("name") != null)
        vNode.setProperty("name", node.get("name").asText());

      if (node.get("represents") != null)
        vNode.setProperty("represents", loadFromIndex(networkIndex, node, "represents").getId());

      vNode.setProperty("jdex_id", index);

      vNode.save();

      network.addEdge("nodes", vNode);

      networkIndex.put(index, vNode);
      nodesCount++;
    }

    network.setProperty("nodesCount", nodesCount);
  }

  private void createProperties(JsonNode networkJDEx, OrientVertex network) {
    Map<String, String> propertiesMap = new HashMap<String, String>();

    JsonNode properties = networkJDEx.get("properties");
    Iterator<String> propertiesIterator = properties.getFieldNames();

    while (propertiesIterator.hasNext()) {
      String index = propertiesIterator.next();
      propertiesMap.put(index, properties.get(index).asText());
    }

    network.setProperty("properties", propertiesMap);
  }

  private void createTerms(JsonNode networkJDEx, OrientBaseGraph orientGraph, OrientVertex network,
      HashMap<String, OrientVertex> networkIndex) {
    final ArrayList<OrientVertex> functions = new ArrayList<OrientVertex>();

    JsonNode terms = networkJDEx.get("terms");
    Iterator<String> termsIterator = terms.getFieldNames();

    while (termsIterator.hasNext()) {
      String index = termsIterator.next();
      JsonNode term = terms.get(index);

      final OrientVertex vTerm;
      if (term.get("name") != null) {
        vTerm = orientGraph.addVertex("xBaseTerm", (String) null);
        vTerm.setProperty("name", term.get("name").asText());

      } else if (term.get("termFunction") != null) {
        vTerm = orientGraph.addVertex("xFunctionTerm", (String) null);

        functions.add(vTerm);
      } else
        continue;

      vTerm.setProperty("jdex_id", index);
      vTerm.save();

      network.addEdge("terms", vTerm);
      networkIndex.put(index, vTerm);
    }

    postProcessFunctions(functions, terms, networkIndex);
  }

  private void postProcessFunctions(List<OrientVertex> functionTerms, JsonNode terms, HashMap<String, OrientVertex> networkIndex) {
    for (OrientVertex functionTerm : functionTerms) {
      final JsonNode jTerm = terms.get((String) functionTerm.getProperty("jdex_id"));
      functionTerm.setProperty("termFunction", loadFromIndex(networkIndex, jTerm, "termFunction").getRecord());

      final Set<ODocument> linkParams = new HashSet<ODocument>();
      final Set<String> textParams = new HashSet<String>();

      final JsonNode jParameters = jTerm.get("parameters");
      final Iterator<String> iterator = jParameters.getFieldNames();
      while (iterator.hasNext()) {
        final String index = iterator.next();
        final JsonNode jParam = jParameters.get(index);
        if (jParam.get("term") != null) {
          linkParams.add(loadFromIndex(networkIndex, jParam, "term").getRecord());
        } else {
          textParams.add(jParam.asText());
        }
      }

      functionTerm.setProperty("linkParameters", linkParams);
      functionTerm.setProperty("textParameters", textParams);
      functionTerm.save();
    }
  }

  private OrientVertex loadFromIndex(HashMap<String, OrientVertex> networkIndex, JsonNode jTerm, String fieldName) {
    final String value = jTerm.get(fieldName).asText();
    final OrientVertex result = networkIndex.get(value);
    if (result == null) {
      throw new RuntimeException("Failed to load " + fieldName + " from index. (id = " + value + ")");
    }
    return result;
  }
}
