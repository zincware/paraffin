import { Stage } from "./types";

// Helper function to determine badge color based on status
export const getStatusBadgeVariant = (status: Stage["status"]) => {
	switch (status) {
		case "pending":
			return "info";
		case "completed":
		case "finished":
			return "success";
		case "running":
			return "primary";
		case "unfinished":
			return "warning";
		case "failed":
			return "danger";
		case "unknown":
		default:
			return "secondary";
	}
};
