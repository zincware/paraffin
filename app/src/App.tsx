import { useState, useEffect, useMemo, useRef } from "react";
import { ReactFlow, Edge, Node, Background, Controls } from "@xyflow/react";
import Dagre from "@dagrejs/dagre";
import GraphStateNode from "./GraphStateNode";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Dropdown from "react-bootstrap/Dropdown";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";

function RangeExample({
	value,
	setValue,
}: { value: number; setValue: (value: number) => void }) {
	return (
		<div className="px-3 py-2">
			<Form.Label style={{ display: "block", marginBottom: "8px" }}>
				<strong>Refresh Interval:</strong> {value} ms
			</Form.Label>
			<Form.Range
				min={100}
				max={60000}
				step={100}
				onChange={(e) => setValue(parseInt(e.target.value))}
				value={value}
				style={{ marginTop: "10px" }}
			/>
		</div>
	);
}

function DropdownMenu({
	value,
	setValue,
}: { value: number; setValue: (value: number) => void }) {
	const handleMenuClick = (e: React.MouseEvent) => {
		// Prevent the dropdown menu from closing
		e.stopPropagation();
	};

	return (
		<Dropdown align="end">
			<Dropdown.Toggle variant="success" id="dropdown-basic">
				Settings
			</Dropdown.Toggle>

			<Dropdown.Menu style={{ minWidth: "300px", padding: "10px" }}>
				<div
					onClick={handleMenuClick}
					style={{ cursor: "default", padding: "0" }}
				>
					<RangeExample value={value} setValue={setValue} />
				</div>
			</Dropdown.Menu>
		</Dropdown>
	);
}

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

// Define TypeScript types for the graph
type GraphNode = {
	id: string;
	label: string;
	status: string;
	queue: string;
	lock: object;
	deps_lock: object;
	deps_hash: string;
	group: string[];
};

type GraphEdge = {
	source: string;
	target: string;
};

type GraphData = {
	edges: GraphEdge[];
	nodes: GraphNode[];
};

// Dagre Graph Layout Configuration
const dagreGraph = new Dagre.graphlib.Graph();
dagreGraph.setDefaultEdgeLabel(() => ({}));

const nodeWidth = 280;
const nodeHeight = 150;

const applyDagreLayout = (nodes: Node[], edges: Edge[], direction = "TB") => {
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
							label: node.label,
							status: node.status,
							width: nodeWidth,
							height: nodeHeight,
							queue: node.queue,
							lock: node.lock,
							deps_lock: node.deps_lock,
							deps_hash: node.deps_hash,
							group: node.group,
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
