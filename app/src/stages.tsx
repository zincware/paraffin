import { useSearchParams } from "react-router-dom";
import { useState, useEffect } from "react";
import { Stage } from "./types";

const StageView = () => {
    const [searchParams] = useSearchParams();
	const experimentId = searchParams.get("experiment");
    const [stages, setStages] = useState<Stage[]>([]);
    const fetchStages = async () => {
        try {
            const response = await fetch(`/api/v1/stages?experiment=${experimentId}`);
            if (!response.ok) {
                throw new Error("Network response was not ok");
            }
            const data = await response.json();
            setStages(data);
        } catch (error) {
            console.error("Error fetching stages:", error);
        }
    }
    // Fetch stages when the component mounts
    useEffect(() => {
        if (experimentId) {
            fetchStages();
        }
    }
    , [experimentId]);
    return (
        <div>
            <h1>Stages</h1>
            <p>Stage view content goes here.</p>
            <table>
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Name</th>
                    </tr>
                </thead>
                <tbody>
                    {stages.map((stage) => (
                        <tr key={stage.id}>
                            <td>{stage.id}</td>
                            <td>{stage.name}</td>
                            <td>{stage.status}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}

export default StageView
