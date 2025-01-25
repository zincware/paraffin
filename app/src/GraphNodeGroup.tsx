import { useCallback, useContext, useEffect, useState } from "react";
import { Handle, Position } from "@xyflow/react";
import Card from "react-bootstrap/Card";
import { GraphNode } from "./types";
import { BsArrowsAngleContract, BsArrowsAngleExpand } from "react-icons/bs";
import { Button } from "react-bootstrap";
import GraphContext from "./GraphContext";

interface GraphStateNodeProps {
	data: {
		width: number;
		height: number;
		node: GraphNode;
	};
}

// TODO: cleanup the data.node and others!

// TODO: on the edge between the nodes, show infos on which attributes are connected.
//  this can later be edited to build the graph in the paraffin ui.

function GraphNodeGroup({ data }: GraphStateNodeProps) {
	const { setHiddenNodes, visibleDepth } = useContext(GraphContext);

	const [excludedNodes, setExcludedNodes] = useState<string[]>([]);

	// TODO: there still seems to be an issue with children of children

	useEffect(() => {
		if (data.depth >= visibleDepth) {
			const children = data.node.children.map((child) => String(child.id));
			setExcludedNodes(children);
			setHiddenNodes((prev) => [...prev, ...children]);
		}
	}, []);

	const onCollapse = useCallback(() => {
		// if excluded nodes length is 0
		if (excludedNodes.length === 0) {
			// set the children of the group as hidden
			const children = data.node.children.map((child) => String(child.id));
			setExcludedNodes(children);
			setHiddenNodes((prev) => [...prev, ...children]);
		} else {
			// remove the children from the hidden nodes
			setHiddenNodes((prev) =>
				prev.filter((node) => !excludedNodes.includes(node)),
			);
			setExcludedNodes([]);
		}
	}, [excludedNodes, setHiddenNodes, data.node.children]);

	return (
		<>
			<div
				style={{
					position: "relative",
					width: data.node.width,
					height: data.node.height,
				}}
			>
				<Handle
					type="target"
					position={Position.Left}
					style={{ background: "black", borderRadius: "50%" }}
				/>
				<Handle
					type="source"
					position={Position.Right}
					id="a"
					style={{ background: "black", borderRadius: "50%" }}
				/>
				<Card
					style={{
						width: data.node.width,
						height: data.node.height,
						borderRadius: "10px",
						boxShadow: "0 6px 12px rgba(0, 0, 0, 0.15)",
						overflow: "hidden",
						border: "3px solid #ddd",
						background: "linear-gradient(135deg, #f9f9f9, #eaeaea)",
						display: "flex",
						flexDirection: "column",
					}}
				>
					{/* Header */}
					<Card.Header
						style={{
							display: "flex",
							justifyContent: "space-between",
							alignItems: "center",
							background: "#f5f5f5",
							padding: "10px 15px",
							fontSize: "1.1rem",
							fontWeight: "600",
							borderBottom: "1px solid #ddd",
						}}
					>
						<span style={{ color: "#333" }}>{data.node.id}</span>
						<Button
							variant="light"
							style={{
								fontSize: "1rem",
								display: "flex",
								alignItems: "center",
								justifyContent: "center",
								padding: "6px 8px",
								boxShadow: "none",
							}}
							onClick={onCollapse}
						>
							{excludedNodes.length === 0 ? (
								<BsArrowsAngleContract />
							) : (
								<BsArrowsAngleExpand />
							)}
						</Button>
					</Card.Header>
					{/* Body */}
					<Card.Body
						style={{
							padding: "15px",
							fontSize: "0.9rem",
							color: "#555",
							flex: "1",
						}}
					>
						<Card.Text style={{ marginBottom: "0" }}>
							{excludedNodes.length > 0 && (
								<span>{excludedNodes.length} children hidden</span>
							)}
						</Card.Text>
					</Card.Body>
				</Card>
			</div>
		</>
	);
}

export default GraphNodeGroup;
