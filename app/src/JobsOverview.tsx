import React, { useEffect, useState } from "react";
import { Table, Card, OverlayTrigger, Tooltip, ProgressBar } from "react-bootstrap";
import { Jobs, WorkerInfo } from "./types";

const JobStatusTable = ({ workerInfo, jobs }: { workerInfo: WorkerInfo[] | null; jobs: Jobs | null }) => {
    const [totalJobs, setTotalJobs] = useState(0);

    useEffect(() => {
        if (!jobs) return;

        const total = jobs.pending + jobs.running + jobs.completed + jobs.cached + jobs.failed;
        console.log("Total Jobs: ", total);
        setTotalJobs(total);
    }
    , [jobs]);

  return (
    <Card className="p-3">
      {/* Progress Bar */}
      <ProgressBar className="mb-3" >
      <ProgressBar variant="danger" now={jobs?.failed} key={1} max={totalJobs} />
      <ProgressBar variant="success" now={jobs?.completed} key={2} max={totalJobs} />
      <ProgressBar variant="info" now={jobs?.running} key={3} max={totalJobs} />
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
            <td>{workerInfo.length}</td>
          </tr>
          <tr>
            <td>Jobs Running</td>
            <td>{jobs?.running}</td>
          </tr>
          <tr>
            <td>Jobs Pending</td>
            <td>{jobs?.pending}</td>
          </tr>
          <tr>
            <td>Jobs Completed</td>
            <td>{jobs?.completed}</td>
          </tr>
          <tr>
            <td>Jobs Failed</td>
            <td>{jobs?.failed}</td>
          </tr>
          <tr>
            <td>Jobs Cached</td>
            <td>{jobs?.cached}</td>
          </tr>
        </tbody>
      </Table>
    </Card>
  );
};

export default JobStatusTable;