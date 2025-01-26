import { useState, useEffect, useContext } from "react";
import { Handle } from "@xyflow/react";
import Card from "react-bootstrap/Card";
import { FaSpinner } from "react-icons/fa";
import Button from "react-bootstrap/Button";
import Modal from "react-bootstrap/Modal";
import Markdown from "react-markdown";
import { GraphNode } from "./types";
import GraphContext from "./GraphContext";

interface GraphStateNodeProps {
	data: {
		width: number;
		height: number;
		node: GraphNode;
	};
}

const statusColors: { [key: string]: string } = {
	pending: "lightgray",
	running: "blue",
	completed: "green",
	failed: "red",
	cached: "purple",
	default: "orange",
};

interface NodeData {
	stdout: string;
	stderr: string;
	started_at: string;
	finished_at: string;
	machine: string;
	worker: string;
}

const updateNodeStauts = (name: string, experiment: string) => {
	fetch("/api/v1/job/update" + "?experiment=" + experiment + "&name=" + name)
};

// TODO: on the edge between the nodes, show infos on which attributes are connected.
//  this can later be edited to build the graph in the paraffin ui.

function GraphStateNode({ data }: GraphStateNodeProps) {
	const [color, setColor] = useState(statusColors.default);
	const [nodeData, setNodeData] = useState<NodeData>({}); // nodeData.stdout / stderr /
	const [show, setShow] = useState(false);
	const { excludedNodes, setExcludedNodes, experiment } = useContext(GraphContext);

	// TODO: fetch all node data here and not via the graph!
	const fetchNodeData = async () => {
		const res = await fetch(
			"/api/v1/job" + "?experiment=" + experiment + "&name=" + data.node.name,
		);
		if (!res.ok) {
			throw new Error(`HTTP error! Status: ${res.status}`);
		}
		const decodedRes = await res.json();
		setNodeData(decodedRes);
	};

	useEffect(() => {
		if (show) {
			fetchNodeData();
		}
	}, [show]);

	useEffect(() => {
		setColor(statusColors[data.node.status] || statusColors.default);
	}, [data.node.status]);

	const handleClose = () => setShow(false);
	const handleShow = () => setShow(true);

	return (
		<>
			<div
				style={{ position: "relative", width: data.width, height: data.height }}
			>
				<Handle
					type="target"
					position={data.targetPosition}
					style={{ background: "black", borderRadius: "50%" }}
				/>
				<Handle
					type="source"
					position={data.sourcePosition}
					id="a"
					style={{ background: "black", borderRadius: "50%" }}
				/>
				<Card
					onClick={handleShow}
					style={{
						width: data.width,
						height: data.height,
						borderRadius: "8px",
						boxShadow: "0 4px 6px rgba(0, 0, 0, 0.1)",
						overflow: "hidden",
						border: `6px solid ${color}`,
						backgroundColor: "white",
					}}
				>
					<Card.Body style={{ padding: "10px" }}>
						<Card.Title
							style={{
								fontSize: "1rem",
								marginBottom: "10px",
								textAlign: "center",
							}}
						>
							{data.node.name}
						</Card.Title>
						<hr />
						<Card.Text style={{ fontSize: "0.85rem", color: "gray" }}>
							<strong>Status:</strong>{" "}
							{data.node.status === "running" ? (
								<span style={{ color: color }}>
									<FaSpinner className="spin-icon" /> Running
								</span>
							) : (
								<span style={{ color: color }}>{data.node.status}</span>
							)}
						</Card.Text>
						{data.node.queue && (
							<Card.Text style={{ fontSize: "0.75rem", marginTop: "5px" }}>
								<strong>Queue:</strong> {data.node.queue}
							</Card.Text>
						)}
						{/* {data.group && (
              <Card.Text style={{ fontSize: "0.75rem", marginTop: "5px" }}>
                <strong>Group:</strong> {data.node.group.join(", ")}
              </Card.Text>
            )} */}
					</Card.Body>
				</Card>
			</div>
			<Modal show={show} onHide={handleClose} size="lg">
				<Modal.Header closeButton>
					<Modal.Title>{data.node.id}</Modal.Title>
				</Modal.Header>
				<Modal.Body>
					<Markdown>
						{`
#### Run this Node
~~~
paraffin worker --job ${data.node.id} --experiment ${experiment}
~~~
#### DVC Stage Lock
~~~dict
${JSON.stringify(data.node.lock, null, 4)}
~~~
#### DVC Stage Dependencies Hash
~~~
${data.node.deps_hash}
~~~
`}
						{/* TODO: show node-meta.json if requested */}
					</Markdown>
					{nodeData.stdout && (
						<>
							<h5>STDOUT</h5>
							<pre>{nodeData.stdout}</pre>
						</>
					)}
					{nodeData.stderr && (
						<>
							<h5>STDERR</h5>
							<pre>{nodeData.stderr}</pre>
						</>
					)}
					{nodeData.worker && (
						<>
							<h5>Worker</h5>
							<pre>{nodeData.worker}@{nodeData.machine}</pre>
							<h5>Started At</h5>
							<pre>{nodeData.started_at}</pre>
							<h5>Finished At</h5>
							<pre>{nodeData.finished_at}</pre>
						</>
					)}


				</Modal.Body>
				<Modal.Footer>
					<Button variant="secondary" onClick={handleClose}>
						Close
					</Button>
					{/* TODO: refresh the page */}
					<Button onClick={() => updateNodeStauts(data.node.id, experiment)}
					>Retry</Button>
				</Modal.Footer>
			</Modal>
		</>
	);
}

export default GraphStateNode;
