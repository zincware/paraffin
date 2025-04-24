import { useSearchParams } from "react-router-dom";
import { useState, useEffect, useRef } from "react";
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
import Form from "react-bootstrap/Form";

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
    const [refreshInterval, setRefreshInterval] = useState<string>("0"); // "0" indicates no autorefresh
    const intervalId = useRef<any | null>(null);

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

    useEffect(() => {
        const interval = parseInt(refreshInterval, 10);
        if (interval > 0 && experimentId) {
            intervalId.current = setInterval(fetchStages, interval * 1000);
        } else {
            clearInterval(intervalId.current);
            intervalId.current = null;
        }

        return () => {
            if (intervalId.current) {
                clearInterval(intervalId.current);
            }
        };
    }, [refreshInterval, experimentId]);

    const handleRefreshIntervalChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
        setRefreshInterval(event.target.value);
    };

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
                            <Row className="mb-3 align-items-center">
                                <Col md="auto">
                                    <h1>Stages for Experiment {experimentId}</h1>
                                </Col>
                                <Col md="auto" className="ms-auto">
                                    <Form.Select
                                        size="sm"
                                        value={refreshInterval}
                                        onChange={handleRefreshIntervalChange}
                                        aria-label="Refresh Interval"
                                        className="me-2"
                                    >
                                        <option value="0">No Auto-Refresh</option>
                                        <option value="1">1 second</option>
                                        <option value="5">5 seconds</option>
                                        <option value="10">10 seconds</option>
                                        <option value="30">30 seconds</option>
                                        <option value="60">1 minute</option>
                                    </Form.Select>
                                </Col>
                            </Row>
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