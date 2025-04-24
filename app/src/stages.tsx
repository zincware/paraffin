import { useSearchParams } from "react-router-dom";
import { useState, useEffect } from "react";
import { Stage, StageDetails } from "./types";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import Modal from "react-bootstrap/Modal";

// Helper function to determine badge color based on status
const getStatusBadgeVariant = (status: Stage["status"]) => {
	switch (status) {
		case "queued":
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


const StageView = () => {
    const [searchParams] = useSearchParams();
    const experimentId = searchParams.get("experiment");
    const [stages, setStages] = useState<Stage[]>([]);
    const [stageDetails, setStageDetails] = useState<StageDetails | null>(null);
    const [showModal, setShowModal] = useState(false);

    const fetchStages = async () => {
        try {
            const response = await fetch(`/api/v1/stages?experiment=${experimentId}`);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const data = await response.json();
            setStages(data);
        } catch (err: any) {
            console.error("Error fetching stages:", err);
        }
    };

    const fetchStageDetails = async (stageId: string) => {
        try {
            const response = await fetch(`/api/v1/stage?id=${stageId}`);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const data = await response.json();
            setStageDetails(data);
        } catch (err: any) {
            console.error(`Error fetching details for stage ${stageId}:`, err);
        }
    };

    useEffect(() => {
        if (experimentId) {
            fetchStages();
        }
    }, [experimentId]);

    const handleShowDetails = (stageId: string) => {
        fetchStageDetails(stageId);
        setShowModal(true);
    };

    const handleCloseModal = () => {
        setShowModal(false);
        setStageDetails(null);
    };

    return (
        <Container className="mt-4">
            <Row className="mb-3">
                <Col>
                    <Card className="shadow-sm">
                        <Card.Body>
                            <Card.Title className="mb-3">
                                <h1>Stages</h1>
                            </Card.Title>
                            <Card.Subtitle className="mb-2 text-muted">
                                Stages for Experiment ID: {experimentId}
                            </Card.Subtitle>
                            <Table striped bordered hover responsive>
                                <thead>
                                    <tr>
                                        <th>ID</th>
                                        <th>Name</th>
                                        <th>Status</th>
                                        <th>Actions</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {stages.map((stage) => (
                                        <tr key={stage.id}>
                                            <td>{stage.id}</td>
                                            <td>{stage.name}</td>
                                            <td>
                                                <Badge pill bg={getStatusBadgeVariant(stage.status)}>
                                                    {stage.status}
                                                </Badge>
                                            </td>
                                            <td>
                                                <Button
                                                    variant="outline-info"
                                                    size="sm"
                                                    onClick={() => handleShowDetails(stage.id)}
                                                >
                                                    Details
                                                </Button>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </Table>
                            {stages.length === 0 && (
                                <div className="text-center mt-3">
                                    <p className="text-muted">No stages found for this experiment.</p>
                                </div>
                            )}
                        </Card.Body>
                    </Card>
                </Col>
            </Row>

            {/* Stage Details Modal */}
            <Modal show={showModal} onHide={handleCloseModal} centered>
                <Modal.Header closeButton>
                    <Modal.Title>Stage Details</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    {stageDetails && (
                        <div>
                            <p><strong>Addressing:</strong> {stageDetails.addressing}</p>
                            <p><strong>Status:</strong> <Badge pill bg={getStatusBadgeVariant(stageDetails.status)}>{stageDetails.status}</Badge></p>
							<p><strong>Command:</strong> {stageDetails.cmd}</p>
							<p><strong>Path:</strong> {stageDetails.path}</p>
							<p><strong>Lockfile:</strong> {stageDetails.lockfile}</p>
                        </div>
                    )}
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" onClick={handleCloseModal}>
                        Close
                    </Button>
                </Modal.Footer>
            </Modal>
        </Container>
    );
};

export default StageView;