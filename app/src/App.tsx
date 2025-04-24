import { BrowserRouter, Route, Routes } from "react-router-dom";
import ExperimentView from "./experiments";
import StageView from "./stages";

const Welcome = () => {
	return (
		<div>
			<h1>Welcome to the App</h1>
			<p>This is the welcome page.</p>
		</div>
	);
};

const App: React.FC = () => {
	return (
		<>
			<BrowserRouter>
				<Routes>
					<Route path="/" element={<Welcome />} />
					<Route path="/experiments" element={<ExperimentView />} />
					<Route path="/stages" element={<StageView />} />
				</Routes>
			</BrowserRouter>
		</>
	);
};

export default App;
