import { useState, useEffect } from "react";
import { Handle, Position } from "@xyflow/react";
import Card from "react-bootstrap/Card";
import { FaSpinner } from "react-icons/fa";

interface GraphStateNodeProps {
  data: {
    label: string;
    status: string;
    queue: string;
    width: number;
    height: number;
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

  return (
    <div style={{ position: "relative", width: data.width, height: data.height }}>
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
          width: data.width,
          height: data.height,
          borderRadius: "8px",
          boxShadow: "0 4px 6px rgba(0, 0, 0, 0.1)",
          overflow: "hidden",
          textAlign: "center",
          border: `6px solid ${color}`,
          backgroundColor: "white",
        }}
      >
        <Card.Body style={{ padding: "10px" }}>
          <Card.Title style={{ fontSize: "1rem", marginBottom: "10px" }}>
            {data.label}
          </Card.Title>
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
        </Card.Body>
      </Card>
    </div>
  );
}

export default GraphStateNode;
