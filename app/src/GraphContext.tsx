import { createContext } from "react";

interface GraphContextType {
	excludedNodes: { [key: string]: string[] };
	setExcludedNodes: (nodes: { [key: string]: string[] }) => void;
	experiment: string;
}

const GraphContext = createContext<GraphContextType>(null);

export default GraphContext;
