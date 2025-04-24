export interface Experiment {
	created_at: string;
	base: string;
	origin: string;
	id: string;
	machine: string;
	status: "active" | "inactive";
}

export interface Stage {
	id: string;
	name: string;
	status:
		| "QUEUED"
		| "COMPLETED"
		| "FINISHED"
		| "RUNNING"
		| "UNFINISHED"
		| "FAILED"
		| "UNKNOWN";
}
