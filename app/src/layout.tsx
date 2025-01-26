import ELK from "elkjs/lib/elk.bundled.js";
import { Edge, Node } from "@xyflow/react";

// Initialize the ELK layout engine
const elk = new ELK();

const applyElkLayout = (nodes: Node[], edges: Edge[], direction = "TB") => {
	// const isHorizontal = options?.['elk.direction'] === 'RIGHT';
	const isHorizontal = true;
	const graph = {
		id: "root",
		layoutOptions: {
			"elk.algorithm": "layered",
			"elk.direction": direction,
			"elk.spacing.nodeNode": 15,
		},
		children: nodes.map((node) => ({
			...node,
			// Adjust the target and source handle positions based on the layout
			// direction.
			targetPosition: isHorizontal ? "left" : "top",
			sourcePosition: isHorizontal ? "right" : "bottom",

			// Hardcode a width and height for elk to use when layouting.
			width: 150,
			height: 50,
		})),
		edges: edges,
	};

	return elk
		.layout(graph)
		.then((layoutedGraph) => ({
			nodes: layoutedGraph.children.map((node) => ({
				...node,
				// React Flow expects a position property on the node instead of `x`
				// and `y` fields.
				position: { x: node.x, y: node.y },
			})),

			edges: layoutedGraph.edges,
		}))
		.catch(console.error);
};

export default applyElkLayout;
