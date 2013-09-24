package org.ndexbio.rest;

import java.util.*;

import com.orientechnologies.orient.core.metadata.schema.OType;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * @since 9/17/13
 */
public class NdexNetworkService {
  public OrientVertex createNetwork(OrientVertex owner, JsonNode networkJDEx, OrientGraph orientGraph) {
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

  public void deleteNetwork(ORID networkRid, OrientGraph orientGraph) {
    ODatabaseDocumentTx databaseDocumentTx = orientGraph.getRawGraph();
    List<ODocument> allNetworkRecords = databaseDocumentTx.query(new OSQLSynchQuery<Object>("TRAVERSE * FROM " + networkRid));

    for (ODocument networkRecord : allNetworkRecords) {
      networkRecord.delete();
    }
  }

  public ObjectNode findNetworks(String searchExpression, int limit, int offset, OrientGraph orientGraph, ObjectMapper objectMapper) {
    int start = offset * limit;

    final String descriptors = "properties.title as title, @rid as rid, out_nodes.size() as nodeCount, ndexEdges.size() as edgeCount";
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

			network.put("title", document.<String>field("title"));
			network.put("jid", convertFromRID(document.<ORID>field("rid", OType.LINK)));
			network.put("nodeCount", document.<Integer>field("nodeCount"));
			network.put("edgesCount", document.<Integer>field("edgesCount"));
		}

		result.put("blockAmount", 5);

		return result;
  }

  public JsonNode getNetwork(ORID networkRid, OrientGraph orientGraph) {
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

  private void readEdges(OrientGraph orientGraph, OrientVertex vNetwork, ObjectMapper mapper, ArrayNode edges) {
    Set<OIdentifiable> dEdges = vNetwork.getProperty("ndexEdges");
    for (OIdentifiable dEdge : dEdges) {
      Edge eEdge = orientGraph.getEdge(dEdge);

      ObjectNode edge = mapper.createObjectNode();
      edges.add(edge);

      Vertex vPredicate = eEdge.getProperty("p");
      edge.put("p", vPredicate.<String> getProperty("jdex_id"));

      Vertex vSubject = eEdge.getVertex(Direction.OUT);
      Vertex vObject = eEdge.getVertex(Direction.IN);

      edge.put("s", vSubject.<String> getProperty("jdex_id"));
      edge.put("o", vObject.<String> getProperty("jdex_id"));
      edge.put("jid", convertFromRID((ORID) eEdge.getId()));
    }
  }

  private void readNodes(OrientGraph orientGraph, OrientVertex vNetwork, ObjectMapper mapper, ObjectNode nodes) {
    Iterable<Edge> eNodes = vNetwork.getEdges(Direction.OUT);
    for (Edge eNode : eNodes) {
      Vertex vNode = eNode.getVertex(Direction.IN);

      ObjectNode node = mapper.createObjectNode();
      nodes.put(vNode.<String> getProperty("jdex_id"), node);

      node.put("name", vNode.<String> getProperty("name"));
      node.put("jid", convertFromRID((ORID) vNode.getId()));

      final OIdentifiable dRepresents = vNode.<OIdentifiable> getProperty("represents");
      if (dRepresents != null) {
        final Vertex vRepresents = orientGraph.getVertex(dRepresents);
        node.put("represents", vRepresents.<String> getProperty("jdex_id"));
      }
    }
  }

  private void readTerms(OrientVertex vNetwork, ObjectMapper mapper, ObjectNode terms) {
    Iterable<Edge> eTerms = vNetwork.getEdges(Direction.OUT, "terms");
    for (Edge eTerm : eTerms) {
      Vertex vTerm = eTerm.getVertex(Direction.IN);

      final ObjectNode term = mapper.createObjectNode();
      terms.put(vTerm.<String> getProperty("jdex_id"), term);

      term.put("name", vTerm.<String> getProperty("name"));
      term.put("jid", convertFromRID((ORID) vTerm.getId()));
      // term.put("nsid", );
    }
  }

  private void readNamespaces(OrientVertex vNetwork, ObjectMapper mapper, ObjectNode namespaces) {
    Iterable<Edge> edges = vNetwork.getEdges(Direction.OUT, "namespaces");
    for (Edge edge : edges) {
      ObjectNode namespace = mapper.createObjectNode();
      Vertex vNs = edge.getVertex(Direction.IN);

      namespaces.put(vNs.<String> getProperty("jdex_id"), namespace);
      namespace.put("prefix", vNs.<String> getProperty("prefix"));
      namespace.put("rid", convertFromRID((ORID) vNs.getId()));
      namespace.put("uri", vNs.<String> getProperty("uri"));
    }
  }

  private String convertFromRID(ORID rid) {
    return rid.toString().replace("#", "C").replace(":", "R");
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

      edge.setProperty("p", loadFromIndex(networkIndex, edgeJDEx, "p"));
      edge.setProperty("n", network);

      edge.save();

      allEdges.add(edge.getRecord());
    }

    network.setProperty("ndexEdges", allEdges);
    network.save();
  }

  private void createCitations(JsonNode networkJDEx, OrientGraph orientGraph, OrientVertex network,
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

  private void createSupports(JsonNode networkJDEx, OrientGraph orientGraph, OrientVertex network,
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

    network.save();
  }

  private void createNameSpaces(OrientVertex network, JsonNode networkJDEx, OrientGraph orientGraph,
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

      vNamespace.setProperty("uri", namespace.get("uri"));

      vNamespace.save();

      network.addEdge("namespaces", vNamespace);

      networkIndex.put(index, vNamespace);
    }
  }

  private void createNodes(OrientVertex network, JsonNode networkJDEx, OrientGraph orientGraph,
      HashMap<String, OrientVertex> networkIndex) {
    JsonNode nodes = networkJDEx.get("nodes");
    Iterator<String> nodesIterator = nodes.getFieldNames();
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
    }
  }

  private void createProperties(JsonNode networkJDEx, Vertex network) {
    Map<String, String> propertiesMap = new HashMap<String, String>();

    JsonNode properties = networkJDEx.get("properties");
    Iterator<String> propertiesIterator = properties.getFieldNames();

    while (propertiesIterator.hasNext()) {
      String index = propertiesIterator.next();
      propertiesMap.put(index, properties.get(index).asText());
    }

    network.setProperty("properties", propertiesMap);
  }

  private void createTerms(JsonNode networkJDEx, OrientGraph orientGraph, Vertex network, HashMap<String, OrientVertex> networkIndex) {
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
      functionTerm.setProperty("termFunction", loadFromIndex(networkIndex, jTerm, "termFunction"));

      final ArrayList<Object> params = new ArrayList<Object>();
      final JsonNode jParameters = jTerm.get("parameters");
      final Iterator<String> iterator = jParameters.getFieldNames();
      while (iterator.hasNext()) {
        final String index = iterator.next();
        final JsonNode jParam = jParameters.get(index);
        if (jParam.get("term") != null) {
          params.add(loadFromIndex(networkIndex, jParam, "term"));
        } else {
          params.add(jParam.asText());
        }
      }

      functionTerm.setProperty("parameters", params);
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
