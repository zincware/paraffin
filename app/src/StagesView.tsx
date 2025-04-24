import { useSearchParams } from "react-router-dom";
import { useState, useEffect } from "react";
import { Stage } from "./types";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import StageDetailView from "./StageDetailView";
import ProgressBar from "react-bootstrap/ProgressBar";
import { getStatusBadgeVariant } from "./utils";

// takes in a dictionary of {status: integer} and returns a component
const StageProgress = ({ status }: { status: Record<string, number> }) => {
    const total = Object.values(status).reduce((acc, val) => acc + val, 0);
    const progress = Object.entries(status).map(([key, value]) => ({
        key,
        value,
        percentage: total > 0 ? (value / total) * 100 : 0, // Avoid division by zero
    }));

    return (
        <ProgressBar>
            {progress.map((item) => (
                <ProgressBar
                    key={item.key}
                    variant={getStatusBadgeVariant(item.key as Stage["status"])}
                    now={item.percentage}
                    label={`${item.key} (${item.value})`}
                />
            ))}
        </ProgressBar>
    );
};

const StageView = () => {
    const [searchParams] = useSearchParams();
    const experimentId = searchParams.get("experiment");
    const [stages, setStages] = useState<Stage[]>([]);
    const [selectedStageId, setSelectedStageId] = useState<string | null>(null);
    const [showModal, setShowModal] = useState(false);
    const [statusCounts, setStatusCounts] = useState<{ [key: string]: number }>({
        pending: 0,
        completed: 0,
        finished: 0,
        running: 0,
        unfinished: 0,
        failed: 0,
        unknown: 0,
    });

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

    useEffect(() => {
        if (experimentId) {
            fetchStages();
        }
    }, [experimentId]);

    useEffect(() => {
        const counts: { [key: string]: number } = {
            pending: 0,
            completed: 0,
            finished: 0,
            running: 0,
            unfinished: 0,
            failed: 0,
            unknown: 0,
        };
        stages.forEach((stage) => {
            counts[stage.status] = (counts[stage.status] || 0) + 1;
        });
        setStatusCounts(counts);
    }, [stages]);

    const handleShowDetails = (stageId: string) => {
        setSelectedStageId(stageId);
        setShowModal(true);
    };

    const handleCloseModal = () => {
        setShowModal(false);
        setSelectedStageId(null); // Reset selected stage ID when modal is closed
    };

    return (
        <Container className="mt-4">
            <Row className="mb-3">
                <Col>
                    <Card className="shadow-sm">
                        <Card.Body>
                            <Card.Title className="mb-3">
                                <h1>Stages for Experiment {experimentId}</h1>
                            </Card.Title>
                            <Card.Subtitle className="mb-2 text-muted">
							{stages.length > 0 && <StageProgress status={statusCounts} />}
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
                                    <p className="text-muted">
                                        No stages found for this experiment.
                                    </p>
                                </div>
                            )}
                        </Card.Body>
                    </Card>
                </Col>
            </Row>

            {/* Stage Details Modal */}
            <StageDetailView
                stageId={selectedStageId}
                show={showModal}
                onClose={handleCloseModal}
            />
        </Container>
    );
};

export default StageView;