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
		| "queued"
		| "completed"
		| "finished"
		| "running"
		| "unfinished"
		| "failed"
		| "unknown";
}

export interface StageDetails {
	addressing: string;
	status:
	| "queued"
	| "completed"
	| "finished"
	| "running"
	| "unfinished"
	| "failed"
	| "unknown";
	cmd: string;
	path: string;
	lockfile: string;
}