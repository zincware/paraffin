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
import "./App.css";

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
	const [visibleDepth, setVisibleDepth] = useState(2);

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

			const availableNodeIds = new Set();

			const processNodes = (node, parent) => {
				if (hiddenNodes.includes(node.id)) {
					// find all edges and set the source to the parent
					rawGraphCopy.edges.forEach((edge) => {
						if (edge.sources[0] === node.id) {
							edge.sources[0] = parent.id;
						}
					});
					return null;
				}

				// Assign default dimensions
				node.width = 280;
				node.height = 150;

				// Process children recursively
				if (node.children) {
					node.labels = [{ text: node.id, width: 100, height: 100 }];
					// node.height = 3000;
					node.children = node.children
						.map((child) => processNodes(child, node)) // Increment depth for children
						.filter((child) => child !== null); // Remove hidden children
				}

				// Track node IDs that are still available
				availableNodeIds.add(node.id);

				return node;
			};

			// Process top-level nodes
			rawGraphCopy.children = rawGraphCopy.children
				.map((node) => processNodes(node, null)) // Start at depth 0 for top-level nodes
				.filter((node) => node !== null); // Remove hidden top-level nodes

			rawGraphCopy.edges = rawGraphCopy.edges.filter((edge) => {
				return (
					availableNodeIds.has(edge.sources[0]) &&
					availableNodeIds.has(edge.targets[0])
				);
			});

			console.log("rawGraphCopy", rawGraphCopy);

			// Run the ELK layout
			elk
				.layout(rawGraphCopy, {
					layoutOptions: {
						"elk.algorithm": "layered", // Change to "box" if necessary
						"elk.layered.spacing.nodeNodeBetweenLayers": "50", // Adjust spacing as needed
						// "org.eclipse.elk.spacing.labelLabel": "100",
						"elk.spacing.componentComponent": "100", // Ensures proper spacing between disconnected components
						"elk.direction": "RIGHT",
						"org.eclipse.elk.hierarchyHandling": "INCLUDE_CHILDREN",
						// "org.eclipse.elk.nodeLabels.placement": "V_CENTER",
						"elk.padding": "[top=75,left=12,bottom=12,right=12]",
					},
					// logging: true,
					// measureExecutionTime: true,
				})
				.then((layoutedGraph) => {
					console.log("layoutedGraph", layoutedGraph);
					setElkGraph(layoutedGraph); // Update the layouted graph state
				});
		}
	}, [rawGraph, hiddenNodes, visibleDepth]);

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
		}
	}, [elkGraph, setNodes, setEdges, hiddenNodes]);

	return (
		<GraphContext.Provider value={{ setHiddenNodes, visibleDepth }}>
			<ReactFlow
				nodes={nodes}
				edges={edges}
				onNodesChange={onNodesChange}
				onEdgesChange={onEdgesChange}
				nodeTypes={nodeTypes}
				minZoom={0.1}
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
