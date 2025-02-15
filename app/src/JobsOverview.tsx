import { useEffect, useState } from "react";
import {
	Table,
	Card,
	ProgressBar,
} from "react-bootstrap";
import { Jobs, WorkerInfo } from "./types";

const JobStatusTable = ({
	workerInfo,
	jobs,
}: { workerInfo: WorkerInfo[] | null; jobs: Jobs | null }) => {
	const [totalJobs, setTotalJobs] = useState(0);

	useEffect(() => {
		if (!jobs) return;

		const total =
			jobs.pending + jobs.running + jobs.completed + jobs.cached + jobs.failed;
		console.log("Total Jobs: ", total);
		setTotalJobs(total);
	}, [jobs]);

	const jobStatusColors = {
		failed: "danger",
		completed: "success",
		running: "info",
		pending: "warning",
		cached: "secondary",
	};

	const jobStatusLabels = {
		failed: "Failed",
		completed: "Completed",
		running: "Running",
		pending: "Pending",
		cached: "Cached",
	};

	return (
		<Card className="p-3">
			{/* Progress Bar */}
			<ProgressBar className="mb-3">
				<ProgressBar
					variant="danger"
					now={jobs?.failed}
					key={1}
					max={totalJobs}
				/>
				<ProgressBar
					variant="success"
					now={jobs?.completed}
					key={2}
					max={totalJobs}
				/>
				<ProgressBar
					variant="info"
					now={jobs?.running}
					key={3}
					max={totalJobs}
				/>
				<ProgressBar
					variant="warning"
					now={jobs?.pending}
					key={4}
					max={totalJobs}
				/>
				<ProgressBar
					variant="secondary"
					now={jobs?.cached}
					key={5}
					max={totalJobs}
				/>
			</ProgressBar>

			{/* Job Status Table */}
			<Table striped bordered hover responsive>
				<thead>
					<tr>
						<th>Category</th>
						<th>Count</th>
					</tr>
				</thead>
				<tbody>
					<tr>
						<td>Workers Online</td>
						<td>{workerInfo?.length}</td>
					</tr>
					{Object.keys(jobStatusLabels).map((status) => (
						<tr key={status}>
							<td
								style={{
									backgroundImage: `linear-gradient(90deg, var(--bs-${jobStatusColors[status]}) 1%, #ffffff00 20%)`,
								}}
							>
								{jobStatusLabels[status]}
							</td>
							<td>{jobs?.[status]}</td>
						</tr>
					))}
				</tbody>
			</Table>
		</Card>
	);
};

export default JobStatusTable;
