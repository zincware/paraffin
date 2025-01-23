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



const nodeWidth = 280;
const nodeHeight = 150;

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
				if (data.nodes && data.edges) {
					const formattedNodes: Node[] = data.nodes.map((node) => ({
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
						position: { x: 0, y: 0 }, // Dagre will calculate positions
						type: "graphstatenode",
					}));

					const formattedEdges: Edge[] = data.edges.map((edge) => ({
						id: `e${edge.source}-${edge.target}`,
						source: edge.source,
						target: edge.target,
					}));

					const layoutedNodes = applyDagreLayout(
						formattedNodes,
						formattedEdges,
            nodeWidth,
            nodeHeight,
					);

					setNodes(layoutedNodes);
					setEdges(formattedEdges);
				} else {
					throw new Error("Invalid graph data format");
				}
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
				width: "80%",
				height: "50vh",
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
