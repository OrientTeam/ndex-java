package org.ndexbio.rest;

import java.util.*;

import org.codehaus.jackson.JsonNode;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * @since 9/17/13
 */
public class NdexNetworkService {
  public void createNetwork(OrientVertex owner, JsonNode networkJDEx, OrientGraph orientGraph) {
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
  }

  private void createEdges(JsonNode networkJDEx, OrientVertex network, HashMap<String, OrientVertex> networkIndex) {
    final ArrayList<OrientEdge> allEdges = new ArrayList<OrientEdge>();

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

      allEdges.add(edge);
    }

    network.setProperty("edges", allEdges);
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
        vNode.setProperty("name", node.get("name"));

      if (node.get("represents") != null)
        vNode.setProperty("represents", loadFromIndex(networkIndex, node, "represents"));

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
