import { useCallback, useContext, useEffect } from "react";
import { Handle } from "@xyflow/react";
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
	const { excludedNodes, setExcludedNodes, experiment } =
		useContext(GraphContext);
	// TODO: no need for visibleDepth, no need to keep using setHiddenNodes

	const onCollapse = useCallback(() => {
		// if excluded nodes length is 0
		if (
			excludedNodes?.[data.node.id]?.length === 0 ||
			!excludedNodes[data.node.id]
		) {
			// set the children of the group as hidden
			const children = data.node.children.map((child) => String(child.id));
			setExcludedNodes((prev) => ({ ...prev, [data.node.id]: children }));
			// setHiddenNodes((prev) => [...prev, ...children]);
		} else {
			// remove the children from the hidden nodes
			// setHiddenNodes((prev) =>
			// 	prev.filter((node) => !excludedNodes[data.node.id].includes(node))
			// );
			setExcludedNodes((prev) => {
				const newExcludedNodes = { ...prev };
				delete newExcludedNodes[data.node.id];
				return newExcludedNodes;
			});
		}
	}, [excludedNodes, data.node.children, setExcludedNodes]);

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
							{/* {excludedNodes.length === 0 ? ( */}
							{excludedNodes?.[data.node.id]?.length === 0 ||
							!excludedNodes[data.node.id] ? (
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
							{excludedNodes?.[data.node.id]?.length > 0 && (
								// TODO: this has to be recursive
								// if the subnode is hidden, the children won't show up though
								//  need a fix
								<span>
									{excludedNodes[data.node.id].length} children hidden
								</span>
							)}
						</Card.Text>
					</Card.Body>
				</Card>
			</div>
		</>
	);
}

export default GraphNodeGroup;
