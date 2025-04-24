import { useState, useEffect } from "react";
import { StageDetails } from "./types";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import Modal from "react-bootstrap/Modal";
import ReactMarkdown from "react-markdown";
import { getStatusBadgeVariant } from "./utils";

interface StageDetailViewProps {
	stageId: string | null; // Receive stageId to fetch details for
	show: boolean; // Renamed from showModal for clarity
	onClose: () => void;
}

const StageDetailView: React.FC<StageDetailViewProps> = ({
	stageId,
	show,
	onClose,
}) => {
	const [stageDetails, setStageDetails] = useState<StageDetails | null>(null);

	const fetchStageDetails = async (id: string) => {
		try {
			const response = await fetch(`/api/v1/stage?id=${id}`);
			if (!response.ok) {
				throw new Error(`HTTP error! status: ${response.status}`);
			}
			const data = await response.json();
			// Convert data.lockfile to a pretty-printed JSON string
			if (data.lockfile) {
				try {
					data.lockfile =
						"```json\n" +
						JSON.stringify(JSON.parse(data.lockfile), null, 2) +
						"\n```";
				} catch (e) {
					console.error("Error parsing lockfile JSON:", e);
					data.lockfile = "Error displaying lockfile.";
				}
			}
			setStageDetails(data);
		} catch (err: any) {
			console.error(`Error fetching details for stage ${id}:`, err);
			setStageDetails(null); // Clear details on error
		}
	};

	useEffect(() => {
		if (stageId) {
			fetchStageDetails(stageId);
		} else {
			setStageDetails(null); // Clear details when stageId is null
		}
	}, [stageId]);

	return (
		<Modal show={show} onHide={onClose} centered>
			<Modal.Header closeButton>
				<Modal.Title>Stage Details</Modal.Title>
			</Modal.Header>
			<Modal.Body>
				{stageDetails && (
					<div>
						<p>
							<strong>Addressing:</strong> {stageDetails.addressing}
						</p>
						<p>
							<strong>Status:</strong>{" "}
							<Badge pill bg={getStatusBadgeVariant(stageDetails.status)}>
								{stageDetails.status}
							</Badge>
						</p>
						<p>
							<strong>Command:</strong> {stageDetails.cmd}
						</p>
						<p>
							<strong>Path:</strong> {stageDetails.path}
						</p>
						{stageDetails.lockfile && (
							<div>
								<strong>Lockfile:</strong>
								<ReactMarkdown children={stageDetails.lockfile} />
							</div>
						)}
						{!stageDetails.lockfile && (
							<p>
								<strong>Lockfile:</strong> Not available.
							</p>
						)}
					</div>
				)}
				{!stageDetails && show && <p>Loading stage details...</p>}
			</Modal.Body>
			<Modal.Footer>
				<Button variant="secondary" onClick={onClose}>
					Close
				</Button>
			</Modal.Footer>
		</Modal>
	);
};

export default StageDetailView;
