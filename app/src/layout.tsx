import Dagre from "@dagrejs/dagre";
import { Edge, Node } from "@xyflow/react";

const applyDagreLayout = (
	nodes: Node[],
	edges: Edge[],
	direction = "TB",
) => {
	// Dagre Graph Layout Configuration
	const dagreGraph = new Dagre.graphlib.Graph();
	dagreGraph.setDefaultEdgeLabel(() => ({}));
	dagreGraph.setGraph({ rankdir: direction });

	// Add nodes to Dagre
	nodes.forEach((node) => {
		dagreGraph.setNode(node.id, { width: node.width as number, height: node.height as number });
	});

	// Add edges to Dagre
	edges.forEach((edge) => {
		dagreGraph.setEdge(edge.source, edge.target);
	});

	// Run Dagre layout
	Dagre.layout(dagreGraph);

	// Find minimum x and y values - No idea why this is necessary!!
	let minX = Infinity;
	let minY = Infinity;

	nodes.forEach((node) => {
		const { x, y } = dagreGraph.node(node.id);
		if (x < minX) minX = x;
		if (y < minY) minY = y;
	});

	// Normalize positions so the graph starts at (0, 0)
	const positionedNodes = nodes.map((node) => {
		const { x, y } = dagreGraph.node(node.id);
		return {
			...node,
			position: {
				x: x - minX - node.width + 200, // Adjust by minX
				y: y - minY - node.height + 200, // Adjust by minY
			},
		};
	});

	return positionedNodes;
};

export default applyDagreLayout;
