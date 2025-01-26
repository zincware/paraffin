// import { initialNodes, initialEdges } from './initialElements.js';
import ELK from "elkjs/lib/elk.bundled.js";
import { useEffect, useState, useMemo, useRef } from "react";
import {
	Background,
	ReactFlow,
	ReactFlowProvider,
	useNodesState,
	useEdgesState,
	Controls,
	Panel,
	Position,
} from "@xyflow/react";

import InputGroup from 'react-bootstrap/InputGroup';
import Form from 'react-bootstrap/Form';


import "@xyflow/react/dist/style.css";
import { Card, Button } from "react-bootstrap";
import Table from "react-bootstrap/Table";
import { FaPlus, FaMinus, FaArrowRight, FaArrowDown, FaRedo } from "react-icons/fa";

import GraphStateNode from "./GraphStateNode";
import GraphNodeGroup from "./GraphNodeGroup";
import GraphContext from "./GraphContext";
import "./App.css";

const elk = new ELK();

async function fetchElkGraph(experiment: string|null) {
	if (experiment === null) {
		return;
	}
	const res = await fetch("/api/v1/graph" + "?experiment=" + experiment);
	if (!res.ok) {
		throw new Error(`HTTP error! Status: ${res.status}`);
	}
	const data = await res.json();
	return data;
}

async function fetchWorkers() {
	const res = await fetch("/api/v1/workers");
	if (!res.ok) {
		throw new Error(`HTTP error! Status: ${res.status}`);
	}
	const data = await res.json();
	return data;
}

interface ElkSettingsProps {
	layoutOptions: Record<string, any>; // Flexible key-value pair object
	setLayoutOptions: (options: Record<string, any>) => void;
  }

  const ElkSettings: React.FC<ElkSettingsProps> = ({ layoutOptions, setLayoutOptions }) => {

	const inputFormRef = useRef<HTMLTextAreaElement>(null);

	const submitForm = () => {
		if (inputFormRef.current) {
			try {
				const newOptions = JSON.parse(inputFormRef.current.value);
				setLayoutOptions(newOptions);
			} catch (e) {
				console.error("Error parsing JSON", e);
			}
		}
	}

	return (
		<>
		<InputGroup>
			<Form.Control
			ref={inputFormRef}
			defaultValue={JSON.stringify(layoutOptions, null, 2)}
			as="textarea"
			rows={10}
			/>
			<Button variant="outline-secondary" id="button-addon1" onClick={submitForm}>
          Submit
        </Button>
		</InputGroup>
		</>
	)
}

function LayoutFlow({ experiment }: { experiment: string | null }) {
	const [nodes, setNodes, onNodesChange] = useNodesState([]);
	const [edges, setEdges, onEdgesChange] = useEdgesState([]);
	// const [hiddenNodes, setHiddenNodes] = useState<string[]>([]);
	const [visibleDepth, setVisibleDepth] = useState(3);
	const [direction, setDirection] = useState("RIGHT");

	const [lastUpdated, setLastUpdated] = useState(Date.now());

	const [excludedNodes, setExcludedNodes] = useState({}); // {node.id: [excluded children ids]}

	const [rawGraph, setRawGraph] = useState(null);

	const [elkGraph, setElkGraph] = useState(null);

	const [layoutOptions, setLayoutOptions] = useState({
		"elk.algorithm": "layered",
		"org.eclipse.elk.hierarchyHandling": "INCLUDE_CHILDREN",
		"elk.padding": "[top=75,left=12,bottom=12,right=12]"
	  });

	const nodeTypes = useMemo(
		() => ({ graphstatenode: GraphStateNode, graphnodegroup: GraphNodeGroup }),
		[],
	);

	useEffect(() => {
		const interval = setInterval(() => {
			fetchWorkers().then((workers) => {
				// iterate all workers, parse `last_seen` and check if newer than lastUpdated.
				// if so, setLastUpdated to that value.
				workers.forEach((worker) => {
					const lastSeen = Date.parse(worker.last_seen);
					if (lastSeen > lastUpdated) {
						setLastUpdated(lastSeen);
						console.log("Updated last seen to", lastSeen);
					}
				});
			});
		}, 5000);
		return () => {
		  clearInterval(interval);
		};
	  }, [lastUpdated]);

	useEffect(() => {
		fetchElkGraph(experiment).then((graph) => {
			setRawGraph(graph);
		});
	}, [experiment, lastUpdated]);

	useEffect(() => {
		// Function to compute excluded nodes by depth
		const computeExcludedNodesByDepth = (
			node,
			parent = "root",
			depth = 0,
			excluded = {},
		) => {
			if (!excluded[parent]) {
				excluded[parent] = [];
			}

			// Exclude nodes beyond the visible depth
			if (depth > visibleDepth) {
				excluded[parent].push(node.id);
			}

			// Process children recursively
			if (node.children) {
				node.children.forEach((child) => {
					computeExcludedNodesByDepth(child, node.id, depth + 1, excluded);
				});
			}

			return excluded;
		};

		// Calculate excluded nodes when rawGraph or visibleDepth changes
		if (rawGraph) {
			const newExcludedNodes = computeExcludedNodesByDepth(rawGraph);
			setExcludedNodes(newExcludedNodes); // Trigger update of excluded nodes
		}
	}, [rawGraph, visibleDepth]); // Recalculate excluded nodes when rawGraph or visibleDepth change

	useEffect(() => {
		if (rawGraph && excludedNodes) {
			const rawGraphCopy = JSON.parse(JSON.stringify(rawGraph));
			const availableNodeIds = new Set();

			// Function to process nodes based on visibility and children
			const processNodes = (node, parent) => {
				const hiddenNodes = excludedNodes[parent?.id] || [];

				// Skip hidden nodes and rewire their edges
				if (hiddenNodes.includes(node.id)) {
					rawGraphCopy.edges.forEach((edge) => {
						if (edge.sources[0] === node.id) {
							edge.sources[0] = parent.id;
						}
						if (edge.targets[0] === node.id) {
							edge.targets[0] = parent.id;
						}
						// Remove self-references
						if (edge.sources[0] === edge.targets[0]) {
							edge.sources = [];
							edge.targets = [];
						}
					});
					return null;
				}

				// Assign default dimensions
				node.width = 280;
				node.height = 150;

				// Recursively process children
				if (node.children) {
					node.labels = [{ text: node.id, width: 100, height: 100 }];
					node.children = node.children
						.map((child) => processNodes(child, node))
						.filter((child) => child !== null); // Remove hidden children
				}

				availableNodeIds.add(node.id);
				return node;
			};

			// Process top-level nodes
			rawGraphCopy.children = rawGraphCopy.children
				.map((node) => processNodes(node, null))
				.filter((node) => node !== null); // Remove hidden top-level nodes

			// Filter edges to only include those with visible source/target nodes
			rawGraphCopy.edges = rawGraphCopy.edges.filter((edge) => {
				return (
					availableNodeIds.has(edge.sources[0]) &&
					availableNodeIds.has(edge.targets[0])
				);
			});

			// Run the ELK layout
			elk
				.layout(rawGraphCopy, {
					layoutOptions: {
						...layoutOptions,
						"elk.direction": direction,
					},
				})
				.then((layoutedGraph) => {
					setElkGraph(layoutedGraph); // Update the layouted graph state
				});
		}
	}, [rawGraph, excludedNodes, direction, layoutOptions]); // Re-run this effect when rawGraph or excludedNodes change

	// Process ELK layout and update React Flow nodes and edges
	useEffect(() => {
		if (elkGraph) {
			// Recursively extract nodes from ELK graph
			const extractNodesAndEdges = (graph, currentDepth: number = 0) => {
				const hiddenNodes = excludedNodes[graph.id] || [];
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
						if (direction === "DOWN") {
							node.data.sourcePosition = Position.Bottom;
							node.data.targetPosition = Position.Top;
						} else {
							node.data.sourcePosition = Position.Right;
							node.data.targetPosition = Position.Left;
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
						let node = {
							id: child.id,
							position: {
								x: child.x,
								y: child.y,
							},
							type: "graphstatenode",
							data: { node: child },
							style: { width: child.width, height: child.height },
						};
						if (direction === "DOWN") {
							node.data.sourcePosition = Position.Bottom;
							node.data.targetPosition = Position.Top;
						} else {
							node.data.sourcePosition = Position.Right;
							node.data.targetPosition = Position.Left;
						}
						if (graph.id !== "root") {
							node.parentId = graph.id;
						}
						resultNodes.push(node);
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
	}, [elkGraph, setNodes, setEdges, direction]);

	return (
		<>
			<GraphContext.Provider
				value={{ excludedNodes, setExcludedNodes, experiment }}
			>
				<ReactFlow
					nodes={nodes}
					edges={edges}
					onNodesChange={onNodesChange}
					onEdgesChange={onEdgesChange}
					nodeTypes={nodeTypes}
					minZoom={0.01}
				>
					<Panel position="top-right">
						<Button onClick={() => setVisibleDepth(visibleDepth + 1)}>
							<FaPlus />
						</Button>

						<Button onClick={() => setVisibleDepth(visibleDepth - 1)}>
							<FaMinus />
						</Button>
						<Button onClick={() => fetchElkGraph(experiment).then((graph) => {setRawGraph(graph);})}>
							<FaRedo />
						</Button>
						{direction === "RIGHT" ? (
							<Button onClick={() => setDirection("DOWN")}>
								<FaArrowDown />
							</Button>
						) : (
							<Button onClick={() => setDirection("RIGHT")}>
								<FaArrowRight />
							</Button>
						)}
					</Panel>
					<Controls />
					<Background />
				</ReactFlow>
			</GraphContext.Provider>
			{/* <ElkSettings layoutOptions={layoutOptions} setLayoutOptions={setLayoutOptions} /> */}
		</>
	);
}

const ExperimentSelector = ({
	setExperiment,
}: { setExperiment: (experiment: string) => void }) => {
	interface Experiment {
		created_at: string;
		base: string;
		origin: string;
		id: string;
		machine: string;
	}

	const [availableExperiments, setAvailableExperiments] = useState<
		Experiment[]
	>([]);

	useEffect(() => {
		fetch("/api/v1/experiments")
			.then((res) => res.json())
			.then((data) => {
				setAvailableExperiments(data);
			});
	}, []);

	useEffect(() => {
		if (availableExperiments.length === 1) {
			setExperiment(availableExperiments[0].id);
		}
	}, [availableExperiments]);

	return (
		<div>
			<h1>Experiment Selector</h1>
			{availableExperiments.length === 0 && (
				<p>No experiments found for the current commit.</p>
			)}
			<Table striped bordered hover>
				<thead>
					<tr>
						<th>Created At</th>
						<th>Base</th>
						<th>Origin</th>
						<th>Machine</th>
					</tr>
				</thead>
				<tbody>
					{availableExperiments.map((experiment) => (
						<tr key={experiment.id}>
							<td>{experiment.created_at}</td>
							<td>{experiment.base}</td>
							<td>{experiment.origin}</td>
							<td>{experiment.machine}</td>
							<td>
								<Button onClick={() => setExperiment(experiment.id)}>
									Select
								</Button>
							</td>
						</tr>
					))}
				</tbody>
			</Table>

			{/* {availableExperiments.map((experiment) => (
				<Button key={experiment} onClick={() => setExperiment(experiment)}>
					{experiment.created_at}
				</Button>
			))} */}
		</div>
	);
};

const App = () => {
	const [experiment, setExperiment] = useState<string | null>(null);

	return experiment === null ? (
		<ExperimentSelector setExperiment={setExperiment} />
	) : (
		<ReactFlowProvider>
			<Card style={{ width: "100%", height: "85vh" }}>
				<LayoutFlow experiment={experiment} />
			</Card>
		</ReactFlowProvider>
	);
};

export default App;
