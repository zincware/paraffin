// import { initialNodes, initialEdges } from './initialElements.js';
import ELK from "elkjs/lib/elk.bundled.js";
import { useEffect, useState, useMemo } from "react";
import {
	Background,
	ReactFlow,
	ReactFlowProvider,
	useNodesState,
	useEdgesState,
	useReactFlow,
	Controls,
} from "@xyflow/react";

import "@xyflow/react/dist/style.css";
import { Card } from "react-bootstrap";
import GraphStateNode from "./GraphStateNode";
import GraphNodeGroup from "./GraphNodeGroup";
import GraphContext from "./GraphContext";

const elk = new ELK();

async function fetchElkGraph() {
	const res = await fetch("/api/v1/graph");
	if (!res.ok) {
		throw new Error(`HTTP error! Status: ${res.status}`);
	}
	const data = await res.json();
	console.log("data", data);
	return data;
}

function LayoutFlow() {
	const [nodes, setNodes, onNodesChange] = useNodesState([]);
	const [edges, setEdges, onEdgesChange] = useEdgesState([]);
	const [hiddenNodes, setHiddenNodes] = useState<string[]>([]);
	const [visibleDepth, setVisibleDepth] = useState(999);
	const { fitView } = useReactFlow();

	const [rawGraph, setRawGraph] = useState(null);

	const [elkGraph, setElkGraph] = useState(null);

	const nodeTypes = useMemo(
		() => ({ graphstatenode: GraphStateNode, graphnodegroup: GraphNodeGroup }),
		[],
	);

	useEffect(() => {
		console.log("fetching graph");
		fetchElkGraph().then((graph) => {
			setRawGraph(graph);
		});
	}, []);

	useEffect(() => {
		if (rawGraph) {
			const rawGraphCopy = JSON.parse(JSON.stringify(rawGraph));
			// Set layout options for ELK
			rawGraphCopy.layoutOptions = {
				"elk.algorithm": "force", // Algorithm for layout (change if needed)
				// Additional layout options can be uncommented as needed:
				// "elk.direction": "TB", // Top-to-Bottom layout
				// "elk.layered.spacing.nodeNodeBetweenLayers": 100, // Vertical spacing
				// "elk.layered.spacing.nodeNode": 300, // Horizontal spacing
				// "elk.layered.considerModelOrder.strategy": "NODES_AND_EDGES", // Reduce crossings
			};

			const availableNodeIds = new Set();

			const processNodes = (node) => {
				// Exclude hidden nodes
				// Exclude nodes beyond the visible depth

				if (hiddenNodes.includes(node.id)) {
					return null;
				}

				// Assign default dimensions
				node.width = 250;
				node.height = 150;

				// Process children recursively
				if (node.children) {
					node.children = node.children
						.map((child) => processNodes(child)) // Increment depth for children
						.filter((child) => child !== null); // Remove hidden children
				}

				// Track node IDs that are still available
				availableNodeIds.add(node.id);

				return node;
			};

			// Process top-level nodes
			rawGraphCopy.children = rawGraphCopy.children
				.map((node) => processNodes(node)) // Start at depth 0 for top-level nodes
				.filter((node) => node !== null); // Remove hidden top-level nodes

			rawGraphCopy.edges = rawGraphCopy.edges.filter((edge) => {
				return (
					availableNodeIds.has(edge.sources[0]) &&
					availableNodeIds.has(edge.targets[0])
				);
			});

			// Run the ELK layout
			elk.layout(rawGraphCopy).then((layoutedGraph) => {
				setElkGraph(layoutedGraph); // Update the layouted graph state
			});
		}
	}, [rawGraph, hiddenNodes, visibleDepth]); // Added `hiddenNodes` as a dependency

	// Process ELK layout and update React Flow nodes and edges
	useEffect(() => {
		if (elkGraph) {
			// Recursively extract nodes from ELK graph
			const extractNodesAndEdges = (graph, currentDepth: number = 0) => {
				const resultNodes = [];
				const resultEdges = [];

				if (hiddenNodes.includes(graph.id)) {
					// TODO iterate all children and set the edges to the parent!
					return { nodes: resultNodes, edges: resultEdges };
				}

				// Extract nodes
				graph.children?.forEach((child) => {
					if (child.children) {
						// Subgraph: Recursively process children

						let node = {
							id: child.id,
							position: {
								x: child.x,
								y: child.y,
							},
							data: { node: child, depth: currentDepth },
							style: { width: child.width, height: child.height },
							type: "graphnodegroup",
						};
						if (graph.id !== "root") {
							node.parentId = graph.id;
						}

						resultNodes.push(node);
						const { nodes: subNodes, edges: subEdges } = extractNodesAndEdges(
							child,
							currentDepth + 1,
						);
						resultNodes.push(...subNodes);
						resultEdges.push(...subEdges);
					} else {
						// Individual node
						resultNodes.push({
							id: child.id,
							position: {
								x: child.x,
								y: child.y,
							},
							type: "graphstatenode",
							data: { node: child },
							style: { width: child.width, height: child.height },
							parentId: graph.id,
							sourcePosition: "right",
							targetPosition: "left",
						});
					}
				});

				// Extract edges
				graph.edges?.forEach((edge) => {
					resultEdges.push({
						id: edge.id,
						source: edge.sources[0], // ELK edges use arrays for sources/targets
						target: edge.targets[0],
					});
				});

				return { nodes: resultNodes, edges: resultEdges };
			};

			// Extract nodes and edges from the processed ELK graph
			const { nodes: layoutedNodes, edges: layoutedEdges } =
				extractNodesAndEdges(elkGraph);

			// Update React Flow state
			setNodes(layoutedNodes);
			setEdges(layoutedEdges);

			// Fit the view to include all nodes
			fitView();
		}
	}, [elkGraph, setNodes, setEdges, fitView, hiddenNodes]);

	return (
		<GraphContext.Provider value={{ setHiddenNodes, visibleDepth }}>
			<ReactFlow
				nodes={nodes}
				edges={edges}
				onNodesChange={onNodesChange}
				onEdgesChange={onEdgesChange}
				nodeTypes={nodeTypes}
				minZoom={0.1}
				fitView
			>
				{/* <Panel position="top-right">
				<button onClick={() => onLayout({ direction: "DOWN" })}>
					vertical layout
				</button>

				<button onClick={() => onLayout({ direction: "RIGHT" })}>
					horizontal layout
				</button>
			</Panel> */}
				<Controls />
				<Background />
			</ReactFlow>
		</GraphContext.Provider>
	);
}
const App = () => (
	<ReactFlowProvider>
		<Card style={{ width: "100%", height: "85vh" }}>
			<LayoutFlow />
		</Card>
	</ReactFlowProvider>
);

export default App;
