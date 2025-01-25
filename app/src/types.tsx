// Define TypeScript types for the graph
export type GraphNode = {
	id: string;
	label: string;
	status: string;
	queue: string;
	lock: object;
	deps_hash: string;
	group: string[];
};

export type GraphEdge = {
	source: string;
	target: string;
};

export type GraphData = {
	edges: GraphEdge[];
	nodes: GraphNode[];
};
