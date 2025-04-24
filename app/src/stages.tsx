import { useSearchParams } from "react-router-dom";
import { useState, useEffect } from "react";
import { Stage } from "./types";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import Badge from "react-bootstrap/Badge";

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
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    const fetchStages = async () => {
        setLoading(true);
        setError(null);
        try {
            const response = await fetch(`/api/v1/stages?experiment=${experimentId}`);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const data = await response.json();
            setStages(data);
        } catch (err: any) {
            console.error("Error fetching stages:", err);
            setError(`Failed to load stages for experiment ID: ${experimentId}`);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        if (experimentId) {
            fetchStages();
        }
    }, [experimentId]);

    if (!experimentId) {
        return (
            <Container className="mt-4">
                <Card className="bg-light text-center shadow-sm">
                    <Card.Body>
                        <Card.Text className="mb-0">
                            No experiment ID specified in the URL.
                        </Card.Text>
                    </Card.Body>
                </Card>
            </Container>
        );
    }

    if (loading) {
        return (
            <Container className="mt-4">
                <Card className="shadow-sm">
                    <Card.Body className="text-center">
                        Loading stages for experiment ID: {experimentId}...
                    </Card.Body>
                </Card>
            </Container>
        );
    }

    if (error) {
        return (
            <Container className="mt-4">
                <Card className="bg-danger text-white shadow-sm">
                    <Card.Body className="text-center">
                        Error: {error}
                    </Card.Body>
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
        </Container>
    );
};

export default StageView;