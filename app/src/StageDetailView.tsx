import { useState, useEffect } from "react";
import { StageDetails } from "./types";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import Modal from "react-bootstrap/Modal";
import ReactMarkdown from "react-markdown";
import { getStatusBadgeVariant } from "./utils";
import { FaCheckCircle, FaExclamationTriangle } from "react-icons/fa";
import { FaClockRotateLeft } from "react-icons/fa6";
import OverlayTrigger from 'react-bootstrap/OverlayTrigger';
import Tooltip from 'react-bootstrap/Tooltip';

interface StageDetailViewProps {
    stageId: string | null;
    show: boolean;
    onClose: () => void;
    onStatusChange?: (stageId: string, newStatus: string) => void;
}

const StageDetailView: React.FC<StageDetailViewProps> = ({
    stageId,
    show,
    onClose,
    onStatusChange,
}) => {
    const [stageDetails, setStageDetails] = useState<StageDetails | null>(null);
    const [loadingDetails, setLoadingDetails] = useState(false);
    const [errorDetails, setErrorDetails] = useState<string | null>(null);
    const [isUpdatingStatus, setIsUpdatingStatus] = useState(false);
    const [updateStatusError, setUpdateStatusError] = useState<string | null>(null);

    const fetchStageDetails = async (id: string) => {
        setLoadingDetails(true);
        setErrorDetails(null);
        try {
            const response = await fetch(`/api/v1/stage?id=${id}`);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const data = await response.json();
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
            setErrorDetails(`Failed to load details for stage ID: ${id}`);
            setStageDetails(null);
        } finally {
            setLoadingDetails(false);
        }
    };

    const handleUpdateStatus = async (newStatus: string) => {
        if (!stageId) {
            return;
        }

        setIsUpdatingStatus(true);
        setUpdateStatusError(null);

        try {
            const response = await fetch(
                `/api/v1/stage/update?stage_id=${stageId}&status=${newStatus}`,
                {
                    method: "GET", // Or POST depending on your API design
                }
            );

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(
                    `Failed to update status to ${newStatus}: ${response.status} - ${errorData?.detail || "Unknown error"}`
                );
            }

            setIsUpdatingStatus(false);
            fetchStageDetails(stageId); // Refetch to update the displayed status
            if (onStatusChange) {
                onStatusChange(stageId, newStatus);
            }
        } catch (error: any) {
            console.error(`Error updating stage status to ${newStatus}:`, error);
            setUpdateStatusError(error.message);
            setIsUpdatingStatus(false);
        }
    };

    useEffect(() => {
        if (stageId && show) {
            fetchStageDetails(stageId);
        } else if (!show) {
            setStageDetails(null);
            setLoadingDetails(false);
            setErrorDetails(null);
            setUpdateStatusError(null);
        }
    }, [stageId, show]);

    return (
        <Modal show={show} onHide={onClose} centered size="lg"> {/* Made the modal large */}
            <Modal.Header closeButton>
                <Modal.Title>Stage Details</Modal.Title>
            </Modal.Header>
            <Modal.Body>
                {loadingDetails && <p>Loading stage details...</p>}
                {errorDetails && <p className="text-danger">{errorDetails}</p>}
                {stageDetails && (
                    <div>
                        <p>
                            <strong>Addressing:</strong> {stageDetails.addressing}
                        </p>
                        <p className="d-flex align-items-center"> {/* Flex container for status and buttons */}
                            <strong>Status:</strong>{" "}
                            <Badge
                                pill
                                bg={getStatusBadgeVariant(stageDetails.status)}
                                className="ms-2"
                            >
                                {stageDetails.status}
                            </Badge>
                            <div className="ms-3"> {/* Container for buttons */}
								<OverlayTrigger
									placement="top"
									overlay={<Tooltip id="tooltip-top">Mark as Finished</Tooltip>}
								>
                                <Button
                                    variant="outline-success"
                                    size="sm"
                                    className="me-2"
                                    onClick={() => handleUpdateStatus("finished")}
                                    disabled={isUpdatingStatus || stageDetails.status === "finished"}
                                >
                                    <FaCheckCircle />
                                </Button>
								</OverlayTrigger>
								<OverlayTrigger
									placement="top"
									overlay={<Tooltip id="tooltip-top">Mark as Unfinished</Tooltip>}
								>
                                <Button
                                    variant="outline-warning"
                                    size="sm"
                                    className="me-2"
                                    onClick={() => handleUpdateStatus("unfinished")}
                                    disabled={isUpdatingStatus || stageDetails.status === "unfinished"}
                                >
                                    <FaExclamationTriangle />
                                </Button>
								</OverlayTrigger>
								<OverlayTrigger
									placement="top"
									overlay={<Tooltip id="tooltip-top">Mark as Pending</Tooltip>}
								>
                                <Button
                                    variant="outline-info"
                                    size="sm"
                                    onClick={() => handleUpdateStatus("pending")}
                                    disabled={isUpdatingStatus || stageDetails.status === "pending"}
                                >
                                    <FaClockRotateLeft />
                                </Button>
								</OverlayTrigger>
                            </div>
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

                        {updateStatusError && (
                            <p className="text-danger mt-2">{updateStatusError}</p>
                        )}
                        {isUpdatingStatus && <p className="mt-2">Updating status...</p>}
                    </div>
                )}
                {!stageDetails && show && !loadingDetails && !errorDetails && (
                    <p>No stage details available.</p>
                )}
            </Modal.Body>
            <Modal.Footer>
                <Button variant="secondary" onClick={onClose} disabled={isUpdatingStatus}>
                    Close
                </Button>
            </Modal.Footer>
        </Modal>
    );
};

export default StageDetailView;