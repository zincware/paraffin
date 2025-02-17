import { Handle } from "@xyflow/react";
import { useContext, useEffect, useState } from "react";
import Accordion from "react-bootstrap/Accordion";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import Modal from "react-bootstrap/Modal";
import { FaSpinner } from "react-icons/fa";
import Markdown from "react-markdown";
import GraphContext from "./GraphContext";
import type { GraphNode } from "./types";

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

interface WorkerData {
	name: string;
	machine: string;
	cwd: string;
	pid: number;
}

interface NodeData {
	stdout: string;
	stderr: string;
	started_at: string;
	finished_at: string;
	machine: string;
	worker?: WorkerData;
}

// TODO: on the edge between the nodes, show infos on which attributes are connected.
//  this can later be edited to build the graph in the paraffin ui.

function GraphStateNode({ data }: GraphStateNodeProps) {
	const [color, setColor] = useState(statusColors.default);
	const [nodeData, setNodeData] = useState<NodeData>({}); // nodeData.stdout / stderr /
	const [show, setShow] = useState(false);
	const { excludedNodes, setExcludedNodes, experiment } =
		useContext(GraphContext);

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
					<Accordion defaultActiveKey={["0", "1", "2", "3", "4"]} alwaysOpen>
						<Accordion.Item eventKey="0">
							<Accordion.Header>Run this Node</Accordion.Header>
							<Accordion.Body>
								<Markdown>
									{`
~~~
paraffin worker --job ${data.node.id} --experiment ${experiment}
~~~
`}
								</Markdown>
							</Accordion.Body>
						</Accordion.Item>
						<Accordion.Item eventKey="1">
							<Accordion.Header>DVC Stage Lock</Accordion.Header>
							<Accordion.Body>
								<Markdown>
									{`
~~~dict
${JSON.stringify(data.node.lock, null, 4)}
~~~
`}
								</Markdown>
							</Accordion.Body>
						</Accordion.Item>

						{nodeData.stdout && (
							<Accordion.Item eventKey="2">
								<Accordion.Header>STDOUT</Accordion.Header>
								<Accordion.Body>
									<pre>{nodeData.stdout}</pre>
								</Accordion.Body>
							</Accordion.Item>
						)}
						{nodeData.stderr && (
							<Accordion.Item eventKey="3">
								<Accordion.Header>STDERR</Accordion.Header>
								<Accordion.Body>
									<pre>{nodeData.stderr}</pre>
								</Accordion.Body>
							</Accordion.Item>
						)}
						{nodeData.worker && (
							<Accordion.Item eventKey="4">
								<Accordion.Header>Worker</Accordion.Header>
								<Accordion.Body>
									<pre>
										{nodeData.worker.name}@{nodeData.worker.machine}
									</pre>
									<h5>Started At</h5>
									<pre>{nodeData.started_at}</pre>
									<h5>Finished At</h5>
									<pre>{nodeData.finished_at}</pre>
									<h5>Working Directory</h5>
									<pre>{nodeData.worker.cwd}</pre>
									<h5>PID</h5>
									<pre>{nodeData.worker.pid}</pre>
								</Accordion.Body>
							</Accordion.Item>
						)}
					</Accordion>
				</Modal.Body>
				<Modal.Footer>
					<Button variant="secondary" onClick={handleClose}>
						Close
					</Button>
					{/* TODO: refresh the page */}
					<Button
						onClick={() => {
							fetch(
								`/api/v1/job/update?experiment=${experiment}&name=${data.node.id}&status=pending`,
							);
						}}
					>
						Retry
					</Button>
					<Button
						onClick={() => {
							fetch(
								`/api/v1/job/update?experiment=${experiment}&name=${data.node.id}&status=pending&force=true`,
							);
						}}
					>
						Force rerun
					</Button>
				</Modal.Footer>
			</Modal>
		</>
	);
}

export default GraphStateNode;
