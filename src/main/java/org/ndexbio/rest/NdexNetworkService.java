package org.ndexbio.rest;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.codehaus.jackson.JsonNode;

import java.util.*;

/**
 * @since 9/17/13
 */
public class NdexNetworkService {
	public void createNetwork(JsonNode networkJDEx, OrientGraph orientGraph) {
		final OrientVertex network = orientGraph.addVertex("xNetwork", (String) null);

		network.setProperty("format", networkJDEx.get("format").asText());

		createNameSpaces(network, networkJDEx, orientGraph);
		createTerms(networkJDEx, orientGraph, network);
		createProperties(networkJDEx, network);
		createNodes(network, networkJDEx, orientGraph);
		createSupports(networkJDEx, orientGraph, network);
		createCitations(networkJDEx, orientGraph, network);

		network.save();
	}

	private void createCitations(JsonNode networkJDEx, OrientGraph orientGraph, OrientVertex network) {
		JsonNode citations = networkJDEx.get("citations");
		Iterator<String> citationsIterator = citations.getFieldNames();
		while (citationsIterator.hasNext()) {
			String index = citationsIterator.next();
			JsonNode citation = citations.get(index);

			OrientVertex vCitation = orientGraph.addVertex("xCitation", (String) null);
			vCitation.setProperty("identifier", citation.get("identifier").asText());
			vCitation.setProperty("type", citation.get("type").asText());
			vCitation.setProperty("title", citation.get("title").asText());
			vCitation.setProperty("contributors", asStringList(citation.get("contributors")));

			vCitation.save();

			network.addEdge("citations", vCitation);
		}
	}

	private List<String> asStringList(JsonNode contributors) {
		List<String> result = new ArrayList<String>();
		for (JsonNode contributor : contributors) {
			result.add(contributor.asText());
		}

		return result;
	}

	private void createSupports(JsonNode networkJDEx, OrientGraph orientGraph, OrientVertex network) {
		JsonNode supports = networkJDEx.get("supports");
		Iterator<String> supportsIterator = supports.getFieldNames();
		while (supportsIterator.hasNext()) {
			String index = supportsIterator.next();
			JsonNode support = supports.get(index);
			OrientVertex vSupport = orientGraph.addVertex("xSupport", (String) null);
			vSupport.setProperty("index", index);
			vSupport.setProperty("text", support.get("text").asText());

			vSupport.save();

			network.addEdge("supports", vSupport);
		}

		network.save();
	}

	private void createNameSpaces(OrientVertex network, JsonNode networkJDEx, OrientGraph orientGraph) {
		JsonNode namespaces = networkJDEx.get("namespaces");
		Iterator<String> namespacesIterator = namespaces.getFieldNames();
		while (namespacesIterator.hasNext()) {
			String index = namespacesIterator.next();
			JsonNode namespace = namespaces.get(index);

			OrientVertex vNamespace = orientGraph.addVertex("xNameSpace", (String) null);
			vNamespace.setProperty("index", index);
			vNamespace.setProperty("prefix", namespace.get("prefix").asText());
			vNamespace.setProperty("uri", namespace.get("uri"));

			vNamespace.save();

			network.addEdge("namespaces", vNamespace);
		}
	}

	private void createNodes(OrientVertex network, JsonNode networkJDEx, OrientGraph orientGraph) {
		JsonNode nodes = networkJDEx.get("nodes");
		Iterator<String> nodesIterator = nodes.getFieldNames();
		while (nodesIterator.hasNext()) {
			String index = nodesIterator.next();
			JsonNode node = nodes.get(index);

			OrientVertex vNode = orientGraph.addVertex("xNode", (String) null);
			if (node.get("name") != null)
				vNode.setProperty("name", node.get("name"));

			vNode.setProperty("index", index);

			vNode.save();

			network.addEdge("nodes", vNode);
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

	private void createTerms(JsonNode networkJDEx, OrientGraph orientGraph, Vertex network) {
		JsonNode terms = networkJDEx.get("terms");
		Iterator<String> termsIterator = terms.getFieldNames();

		while (termsIterator.hasNext()) {
			String index = termsIterator.next();
			JsonNode term = terms.get(index);

			if (term.get("name") != null) {

				OrientVertex vTerm = orientGraph.addVertex("xBaseTerm", (String) null);
				vTerm.setProperty("index", index);
				vTerm.setProperty("name", term.get("name").asText());
				vTerm.save();

				network.addEdge("terms", vTerm);
			} else if (term.get("termFunction") != null && term.get("termFunction").asBoolean()) {
				OrientVertex vTerm = orientGraph.addVertex("xFunctionTerm", (String) null);

				vTerm.setProperty("index", index);
				vTerm.save();

				network.addEdge("terms", vTerm);
			}
		}
	}
}