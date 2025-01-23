import { useState, useEffect } from "react";
import { Handle, Position } from "@xyflow/react";
import Card from "react-bootstrap/Card";
import { FaSpinner } from "react-icons/fa";
import Button from "react-bootstrap/Button";
import Modal from "react-bootstrap/Modal";
import Markdown from "react-markdown";

interface GraphStateNodeProps {
	data: {
		label: string;
		status: string;
		queue: string;
		width: number;
		height: number;
		lock: object;
		deps_lock: object;
		deps_hash: string;
		group: string[];
	};
}

const statusColors: { [key: string]: string } = {
	pending: "lightgray",
	running: "blue",
	completed: "green",
	failed: "red",
	default: "white",
};

function GraphStateNode({ data }: GraphStateNodeProps) {
	const [color, setColor] = useState(statusColors.default);

	useEffect(() => {
		setColor(statusColors[data.status] || statusColors.default);
	}, [data.status]);

	const [show, setShow] = useState(false);

	const handleClose = () => setShow(false);
	const handleShow = () => setShow(true);

	return (
		<>
			<div
				style={{ position: "relative", width: data.width, height: data.height }}
			>
				<Handle
					type="target"
					position={Position.Top}
					style={{ background: "black", borderRadius: "50%" }}
				/>
				<Handle
					type="source"
					position={Position.Bottom}
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
							{data.label}
						</Card.Title>
						<hr />
						<Card.Text style={{ fontSize: "0.85rem", color: "gray" }}>
							<strong>Status:</strong>{" "}
							{data.status === "running" ? (
								<span style={{ color: color }}>
									<FaSpinner className="spin-icon" /> Running
								</span>
							) : (
								<span style={{ color: color }}>{data.status}</span>
							)}
						</Card.Text>
						{data.queue && (
							<Card.Text style={{ fontSize: "0.75rem", marginTop: "5px" }}>
								<strong>Queue:</strong> {data.queue}
							</Card.Text>
						)}
            {/* {data.group && (
              <Card.Text style={{ fontSize: "0.75rem", marginTop: "5px" }}>
                <strong>Group:</strong> {data.group.join(", ")}
              </Card.Text>
            )} */}
					</Card.Body>
				</Card>
			</div>
			<Modal show={show} onHide={handleClose} size="lg">
				<Modal.Header closeButton>
					<Modal.Title>{data.label}</Modal.Title>
				</Modal.Header>
				<Modal.Body>
					<Markdown>
						{`
  #### DVC Stage Lock
~~~dict
${JSON.stringify(data.lock, null, 4)}
~~~
#### DVC Stage Dependencies Lock
~~~dict
${JSON.stringify(data.deps_lock, null, 4)}
~~~
#### DVC Stage Dependencies Hash
~~~
${data.deps_hash}
~~~
`}
						{/* TODO: show node-meta.json if requested */}
					</Markdown>
				</Modal.Body>
				<Modal.Footer>
					<Button variant="secondary" onClick={handleClose}>
						Close
					</Button>
				</Modal.Footer>
			</Modal>
		</>
	);
}

export default GraphStateNode;
