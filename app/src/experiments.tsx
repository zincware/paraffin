import { Experiment } from "./types";
import { useState, useEffect } from "react";

const ExperimentView = () => {
	const [experiments, setExperiments] = useState<Experiment[]>([]);

	const fetchExperiments = async () => {
		try {
			const response = await fetch("/api/v1/experiments");
			if (!response.ok) {
				throw new Error("Network response was not ok");
			}
			const data = await response.json();
			setExperiments(data);
		} catch (error) {
			console.error("Error fetching experiments:", error);
		}
	};

	// Fetch experiments when the component mounts
	useEffect(() => {
		fetchExperiments();
	}, []);

	return (
		<div>
			<h1>Experiments</h1>
			<p>Experiment view content goes here.</p>
			<table>
				<thead>
					<tr>
						<th>ID</th>
						<th>Created At</th>
						<th>Base</th>
						<th>Origin</th>
						<th>Machine</th>
						<th>Status</th>
					</tr>
				</thead>
				<tbody>
					{experiments.map((experiment) => (
						<tr key={experiment.id}>
							<td>{experiment.id}</td>
							<td>{experiment.created_at}</td>
							<td>{experiment.base}</td>
							<td>{experiment.origin}</td>
							<td>{experiment.machine}</td>
							<td>{experiment.status}</td>
							<td>
								<a href={`/stages?experiment=${experiment.id}`}>View Stages</a>
							</td>
						</tr>
					))}
				</tbody>
			</table>
		</div>
	);
};

export default ExperimentView;
