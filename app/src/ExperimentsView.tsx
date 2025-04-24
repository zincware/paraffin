import { Experiment } from "./types";
import { useState, useEffect, useMemo, useRef } from "react";
import Table from "react-bootstrap/Table";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Card from "react-bootstrap/Card";
import Badge from "react-bootstrap/Badge";
import Form from "react-bootstrap/Form";

// Helper function to format dates nicely
const formatDate = (dateString: string | undefined) => {
	if (!dateString) return "-";
	const date = new Date(dateString);
	return date.toLocaleDateString(undefined, {
		year: "numeric",
		month: "long",
		day: "numeric",
		hour: "2-digit",
		minute: "2-digit",
		second: "2-digit",
	});
};

// Helper function to determine badge color based on status
const getStatusBadgeVariant = (status: string) => {
	switch (status.toLowerCase()) {
		case "active":
			return "success";
		case "inactive":
			return "warning";
		default:
			return "secondary";
	}
};

const ExperimentView = () => {
	const [experiments, setExperiments] = useState<Experiment[]>([]);
	const [loading, setLoading] = useState(true);
	const [error, setError] = useState<string | null>(null);
	const [refreshInterval, setRefreshInterval] = useState<string>("60"); // Default to 5 seconds
	const [autoRefresh, setAutoRefresh] = useState(true); // Default to true
	const intervalId = useRef<any | null>(null);

	const fetchExperiments = async () => {
		setLoading(true);
		setError(null);
		try {
			const response = await fetch("/api/v1/experiments");
			if (!response.ok) {
				throw new Error(`HTTP error! status: ${response.status}`);
			}
			const data = await response.json();
			setExperiments(data);
		} catch (err: any) {
			console.error("Error fetching experiments:", err);
			setError("Failed to load experiments.");
		} finally {
			setLoading(false);
		}
	};

	useEffect(() => {
		fetchExperiments();
	}, []);

	useEffect(() => {
		const interval = parseInt(refreshInterval, 10);
		if (autoRefresh && interval > 0) {
			intervalId.current = setInterval(fetchExperiments, interval * 1000);
		} else {
			clearInterval(intervalId.current);
			intervalId.current = null;
		}

		return () => {
			if (intervalId.current) {
				clearInterval(intervalId.current);
			}
		};
	}, [autoRefresh, refreshInterval]);

	const handleRefreshIntervalChange = (
		event: React.ChangeEvent<HTMLSelectElement>,
	) => {
		setRefreshInterval(event.target.value);
		// If the interval changes, we should restart the auto-refresh timer
		setAutoRefresh(true);
	};

	const sortedExperiments = useMemo(() => {
		return [...experiments].sort((a, b) => {
			if (a.id < b.id) return 1;
			if (a.id > b.id) return -1;
			return 0;
		});
	}, [experiments]);

	if (loading) {
		return (
			<Container className="mt-4">
				<Card className="shadow-sm">
					<Card.Body className="text-center">Loading experiments...</Card.Body>
				</Card>
			</Container>
		);
	}

	if (error) {
		return (
			<Container className="mt-4">
				<Card className="bg-danger text-white shadow-sm">
					<Card.Body className="text-center">Error: {error}</Card.Body>
				</Card>
			</Container>
		);
	}

	return (
		<Container className="mt-4">
			<Row className="mb-3">
				<Col>
					<Card className="shadow-sm">
						<Card.Body>
							<Row className="mb-3 align-items-center">
								<Col>
									<h1>Experiments</h1>
								</Col>
								<Col md="auto" className="text-end">
									<Form.Select
										size="sm"
										value={refreshInterval}
										onChange={handleRefreshIntervalChange}
										aria-label="Refresh Interval"
									>
										<option value="0">No Auto-Refresh</option>
										<option value="10">10 seconds</option>
										<option value="30">30 seconds</option>
										<option value="60">1 minute</option>
									</Form.Select>
								</Col>
							</Row>
							<Card.Text className="text-muted">
								A list of all experiments. Click "View Stages" for more details.
							</Card.Text>
							<Table striped bordered hover responsive>
								<thead>
									<tr>
										<th>ID</th>
										<th>Created At</th>
										<th>Base</th>
										<th>Origin</th>
										<th>Machine</th>
										<th>Status</th>
										<th>Actions</th>
									</tr>
								</thead>
								<tbody>
									{sortedExperiments.map((experiment) => (
										<tr key={experiment.id}>
											<td>{experiment.id}</td>
											<td>{formatDate(experiment.created_at)}</td>
											<td>{experiment.base}</td>
											<td>{experiment.origin}</td>
											<td>{experiment.machine}</td>
											<td>
												<Badge
													pill
													bg={getStatusBadgeVariant(experiment.status)}
												>
													{experiment.status}
												</Badge>
											</td>
											<td>
												<a
													href={`/stages?experiment=${experiment.id}`}
													className="btn btn-sm btn-outline-primary"
												>
													View Stages
												</a>
											</td>
										</tr>
									))}
								</tbody>
							</Table>
							{experiments.length === 0 && (
								<div className="text-center mt-3">
									<p className="text-muted">No experiments found.</p>
								</div>
							)}
						</Card.Body>
					</Card>
				</Col>
			</Row>
		</Container>
	);
};

export default ExperimentView;
