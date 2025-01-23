import Dagre from "@dagrejs/dagre";
import { Edge, Node } from "@xyflow/react";

// Dagre Graph Layout Configuration
const dagreGraph = new Dagre.graphlib.Graph();
dagreGraph.setDefaultEdgeLabel(() => ({}));



const applyDagreLayout = (nodes: Node[], edges: Edge[], nodeWidth: number, nodeHeight: number, direction = "TB", ) => {
	dagreGraph.setGraph({ rankdir: direction });

	// Add nodes to Dagre
	nodes.forEach((node) => {
		dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight });
	});

	// Add edges to Dagre
	edges.forEach((edge) => {
		dagreGraph.setEdge(edge.source, edge.target);
	});

	// Run Dagre layout
	Dagre.layout(dagreGraph);

	// Apply positions to nodes
	const positionedNodes = nodes.map((node) => {
		const nodeWithPosition = dagreGraph.node(node.id);
		return {
			...node,
			position: {
				x: nodeWithPosition.x - nodeWidth / 2,
				y: nodeWithPosition.y - nodeHeight / 2,
			},
		};
	});

	return positionedNodes;
};

export default applyDagreLayout;