import { useCallback, useContext, useEffect, useState } from "react";
import { Handle, Position } from "@xyflow/react";
import Card from "react-bootstrap/Card";
import { GraphNode } from "./types";
import { BsArrowsAngleContract } from "react-icons/bs";
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
					style={{
						width: data.node.width,
						height: data.node.height,
						borderRadius: "8px",
						boxShadow: "0 4px 6px rgba(0, 0, 0, 0.1)",
						overflow: "hidden",
						border: `6px solid grey`,
						backgroundColor: "rgba(255, 255, 255, 0.1)",
						// add backdrop filter blur
						// backdropFilter: "blur(10px)",
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
							{data.node.id}
							<Button
								variant="light"
								style={{ float: "right" }}
								onClick={onCollapse}
							>
								<BsArrowsAngleContract />
							</Button>
						</Card.Title>
					</Card.Body>
				</Card>
			</div>
		</>
	);
}

export default GraphNodeGroup;
