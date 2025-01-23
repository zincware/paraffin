import { useState, useEffect, useMemo, useRef } from "react";
import { ReactFlow, Edge, Node, Background, Controls } from "@xyflow/react";
import GraphStateNode from "./GraphStateNode";
import Card from "react-bootstrap/Card";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import applyDagreLayout from "./layout";
import { GraphData } from "./types";

import DropdownMenu from "./menu";

function useInterval(callback: () => void, delay: number | null) {
	const savedCallback = useRef();

	// Remember the latest callback.
	useEffect(() => {
		savedCallback.current = callback;
	}, [callback]);

	// Set up the interval.
	useEffect(() => {
		function tick() {
			savedCallback.current();
		}
		if (delay !== null) {
			let id = setInterval(tick, delay);
			return () => clearInterval(id);
		}
	}, [delay]);
}

const nodeWidth = 200;
const nodeHeight = 150;

function handleGraphData(data: GraphData) {
	if (data.nodes && data.edges) {
		const groupNodesMap: Record<string, Node> = {}; // To store unique group nodes
		const groupedNodes: Record<string, Node[]> = {}; // To store nodes grouped by group path
		const groupEdgesSet: Set<string> = new Set(); // To track unique edges between groups

		data.nodes.forEach((node) => {
			let parentGroupId = "__default";
			if (node.group && node.group.length > 0) {
				node.group.forEach((group, index) => {
					const groupId = node.group.slice(0, index + 1).join("/"); // Create a unique ID for the group path (e.g., "A/B")
					if (!groupNodesMap[groupId]) {
						groupNodesMap[groupId] = {
							id: `group-${groupId}`,
							data: {
								label: group,
								groupPath: node.group.slice(0, index + 1), // Full group path up to this point
							},
							position: { x: 0, y: 0 }, // Dagre will calculate positions
							width: 1500,
							height: 500,
							type: "group",
						};
					}
					// Add an edge between parent and current group
					if (parentGroupId) {
						const groupEdgeId = `group-${groupId}-to-group-${parentGroupId}`;
						groupEdgesSet.add(groupEdgeId);
					}
					parentGroupId = groupId;
				});
			}

			let formattedNode = {
				id: node.id,
				data: {
					node: {
						id: node.id,
						label: node.label,
						status: node.status,
						queue: node.queue,
						lock: node.lock,
						deps_lock: node.deps_lock,
						deps_hash: node.deps_hash,
						group: node.group,
					},
					height: nodeHeight,
					width: nodeWidth,
				},
				position: {
					x: 0,
					y: 0,
				},
				type: "graphstatenode",
			};

			if (parentGroupId !== "__default") {
				formattedNode.parentId = `group-${parentGroupId}`;
				formattedNode.extent = "parent";
			}

			groupedNodes[parentGroupId] = groupedNodes[parentGroupId] || [];
			groupedNodes[parentGroupId].push(formattedNode);
		});

		// Step 4: Create formatted edges
		const formattedEdges: Edge[] = data.edges
			.map((edge) => {
				const sourceNode = data.nodes.find((n) => n.id === edge.source);
				const targetNode = data.nodes.find((n) => n.id === edge.target);

				if (!sourceNode || !targetNode) return null; // Ignore invalid edges

				// Add an edge between the actual nodes
				return {
					id: `e${edge.source}-${edge.target}`,
					source: edge.source,
					target: edge.target,
				};
			})
			.filter(Boolean) as Edge[];

		// Step 5: Add edges between groups
		const groupEdges: Edge[] = Array.from(groupEdgesSet).map((groupEdgeId) => {
			const [sourceGroupId, targetGroupId] = groupEdgeId.split("-to-");
			return {
				id: `e${sourceGroupId}-${targetGroupId}`,
				source: sourceGroupId,
				target: targetGroupId,
			};
		});

		let layoutedNodes: Node[] = [];

		for (const [groupId, nodes] of Object.entries(groupedNodes)) {
			console.log("formatting group", groupId);
			console.log(nodes);
			const layoutedGroupNodes = applyDagreLayout(
				nodes,
				formattedEdges,
				nodeWidth,
				nodeHeight,
			);
			layoutedNodes.push(...layoutedGroupNodes);
			console.log(layoutedGroupNodes);
			console.log("done.");
		}

		// group nodes must be bevore the other nodes
		layoutedNodes.unshift(
			...applyDagreLayout(
				Object.values(groupNodesMap),
				groupEdges,
				1500,
				500,
				"LR",
			),
		);

		console.log(layoutedNodes);
		console.log(groupedNodes);

		return { nodes: layoutedNodes, edges: formattedEdges };
	} else {
		throw new Error("Invalid graph data format");
	}
}

function App() {
	const [nodes, setNodes] = useState<Node[]>([]);
	const [edges, setEdges] = useState<Edge[]>([]);
	const [loading, setLoading] = useState(true);
	const [error, setError] = useState<string | null>(null);
	const [refreshInterval, setRefreshInterval] = useState(2000);

	const nodeTypes = useMemo(() => ({ graphstatenode: GraphStateNode }), []);

	useInterval(() => {
		fetch("/api/v1/graph")
			.then((res) => {
				if (!res.ok) {
					throw new Error(`HTTP error! Status: ${res.status}`);
				}
				return res.json();
			})
			.then((data: GraphData) => {
				const { nodes, edges } = handleGraphData(data);
				setNodes(nodes);
				setEdges(edges);

				setLoading(false);
			})
			.catch((err) => {
				setError(err.message);
				setLoading(false);
			});
	}, refreshInterval);
	// TODO: this can be very slow for large graphs!
	// TODO: trigger useInterval on component mount as well

	if (loading) return <p>Loading...</p>;
	if (error) return <p>Error: {error}</p>;

	return (
		<Card
			style={{
				width: "90%",
				height: "95vh",
				margin: "auto",
				paddingBottom: "35px",
				marginTop: "20px",
			}}
		>
			<Card.Body>
				<Card.Title>
					<Row className="align-items-center">
						<Col>
							<h3 style={{ fontWeight: "bold", marginBottom: "0" }}>
								Paraffin Graph Interface
							</h3>
						</Col>
						<Col className="text-end">
							<DropdownMenu
								value={refreshInterval}
								setValue={setRefreshInterval}
							/>
						</Col>
					</Row>
				</Card.Title>
				<ReactFlow
					nodeTypes={nodeTypes}
					nodes={nodes}
					edges={edges}
					minZoom={0.01}
				>
					<Background />
					<Controls />
				</ReactFlow>
			</Card.Body>
		</Card>
	);
}

export default App;
