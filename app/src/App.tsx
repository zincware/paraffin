import { BrowserRouter, Route, Routes } from "react-router-dom";
import ExperimentView from "./ExperimentsView";
import StageView from "./StagesView";

const App: React.FC = () => {
	return (
		<>
			<BrowserRouter>
				<Routes>
					<Route path="/" element={<ExperimentView />} />
					<Route path="/experiments" element={<ExperimentView />} />
					<Route path="/stages" element={<StageView />} />
				</Routes>
			</BrowserRouter>
		</>
	);
};

export default App;
